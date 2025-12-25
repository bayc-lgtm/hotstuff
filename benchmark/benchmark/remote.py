from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from os.path import basename, splitext, relpath
from time import sleep
from math import ceil
import subprocess, base64, urllib3

from benchmark.config import (
    Committee,
    Key,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


def _upload_file_content(connection, local_path, remote_dir):
    """Upload a file by reading content and creating it remotely (workaround for no SFTP support)."""
    with open(local_path, 'r') as f:
        content = f.read()
    remote_path = f"{remote_dir}/{basename(local_path)}"
    encoded = base64.b64encode(content.encode()).decode()
    connection.run(f"sudo bash -c 'echo {encoded} | base64 -d > {remote_path}'", hide=True)

def _download_file(host, port, remote_dir, remote_file, local_file):
    """Download a file via HTTP served by miniserve."""
    url = f'http://{host}:{port}/{relpath(remote_file, remote_dir)}'
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    if response.status != 200:
        raise BenchError(f"Failed to download file from {url} (status code: {response.status})")
    with open(local_file, 'wb') as f:
        f.write(response.data)

class FabricError(Exception):
    """Wrapper for Fabric exception with a meaningfull error message."""

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        if self.settings.auth.type == 'teleport':
            sudo = True
            workspace = f'/root/{self.settings.repo_name}/benchmark'
        else:
            sudo = False
            workspace = f'.'
        self.command_maker = CommandMaker(workspace, sudo)
        self.local_command_maker = CommandMaker(f'.', False)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info("Installing rust and cloning the repo...")
        cmd = [
            "sudo apt-get update",
            "sudo apt-get -y upgrade",
            "sudo apt-get -y autoremove",
            # The following dependencies prevent the error: [error: linker `cc` not found].
            "sudo apt-get -y install build-essential",
            "sudo apt-get -y install cmake",
            # Install rust (non-interactive).
            'sudo curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y',
            # Following two commands don't work with sudo
            # "source $HOME/.cargo/env",
            # "rustup default stable",
            # This is missing from the Rocksdb installer (needed for Rocksdb).
            "sudo apt-get install -y clang",
            # We need this for downloading logs from teleport
            "sudo /root/.cargo/bin/cargo install miniserve",
            # Clone the repo.
            f"(sudo git clone {self.settings.repo_url} /root/{self.settings.repo_name} ; sudo git -C /root/{self.settings.repo_name} pull)",
        ]
        connections = self._connections()
        try:
            g = Group.from_connections(connections)
            g.run(" && ".join(cmd), hide=True)
            Print.heading(f"Initialized testbed of {len(connections)} nodes")
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError("Failed to install repo on testbed", e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        connections = self._connections(hosts)
        delete_logs = self.command_maker.clean_logs() if delete_logs else "true"
        cmd = [delete_logs, f"({self.command_maker.kill()} || true)"]
        try:
            g = Group.from_connections(connections)
            g.run(" && ".join(cmd), hide=True)
        except GroupException as e:
            raise BenchError("Failed to kill nodes", FabricError(e))

    def _connections(self, hosts: list[str] = []) -> list[Connection]:
        if not hosts:
            region_hosts = self.manager.hosts()
            hosts = [ip for ips in region_hosts.values() for ip in ips]

        return [self._connection(host) for host in hosts]

    def _connection(self, host: str) -> Connection:
        return Connection(host=host, user=self.settings.auth.user, connect_kwargs=self.settings.auth.connect_kwargs(host))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'sudo tmux new -d -s "{name}" "{command} |& sudo tee {log_file}"'
        c = self._connection(host)
        output = c.run(cmd, hide=True)
        self._check_stderr(output)

    def _update(self, hosts):
        Print.info(f'Updating {len(hosts)} nodes (branch "{self.settings.branch}")...')
        cmd = [
            f"sudo git -C /root/{self.settings.repo_name} fetch -f",
            f"sudo git -C /root/{self.settings.repo_name} checkout -f {self.settings.branch}",
            f"sudo git -C /root/{self.settings.repo_name} pull -f",
            # "source $HOME/.cargo/env",
            self.command_maker.compile(),
            self.command_maker.alias_binaries(f"/root/{self.settings.repo_name}/target/release"),
        ]
        g = Group.from_connections(self._connections(hosts))
        g.run(" && ".join(cmd), hide=True)

    def _config(self, hosts, node_parameters):
        Print.info("Generating configuration files...")

        # Cleanup all local configuration files.
        cmd = self.local_command_maker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = self.local_command_maker.compile().split()
        subprocess.run(cmd, check=True, cwd=self.local_command_maker.path_maker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmds = self.local_command_maker.alias_binaries(self.local_command_maker.path_maker.binary_path())
        subprocess.run(cmds, shell=True)

        # Generate configuration files.
        keys = []
        key_files = [self.local_command_maker.path_maker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = self.local_command_maker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        consensus_addr = [f"{x}:{self.settings.consensus_port}" for x in hosts]
        front_addr = [f"{x}:{self.settings.front_port}" for x in hosts]
        mempool_addr = [f"{x}:{self.settings.mempool_port}" for x in hosts]
        committee = Committee(names, consensus_addr, front_addr, mempool_addr)
        committee.print(self.local_command_maker.path_maker.committee_file())

        node_parameters.print(self.local_command_maker.path_maker.parameters_file())

        # Cleanup all nodes.
        cmd = f"{self.command_maker.cleanup()} || true"
        g = Group.from_connections(self._connections(hosts))
        g.run(cmd, hide=True)

        # Upload configuration files.
        progress = progress_bar(hosts, prefix="Uploading config files:")
        for i, host in enumerate(progress):
            c = self._connection(host)
            _upload_file_content(c, self.local_command_maker.path_maker.committee_file(), self.command_maker.workspace)
            _upload_file_content(c, self.local_command_maker.path_maker.key_file(i), self.command_maker.workspace)
            _upload_file_content(c, self.local_command_maker.path_maker.parameters_file(), self.command_maker.workspace)

        return committee

    def _run_single(self, hosts, rate, bench_parameters, node_parameters, debug=False):
        Print.info("Booting testbed...")

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(self.local_command_maker.path_maker.committee_file())
        addresses = [f"{x}:{self.settings.front_port}" for x in hosts]
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        timeout = node_parameters.timeout_delay
        client_logs = [self.command_maker.path_maker.client_log_file(i) for i in range(len(hosts))]
        for host, addr, log_file in zip(hosts, addresses, client_logs):
            cmd = self.command_maker.run_client(
                addr, bench_parameters.tx_size, rate_share, timeout, nodes=addresses
            )
            self._background_run(host, cmd, log_file)

        # Run the nodes.
        key_files = [self.command_maker.path_maker.key_file(i) for i in range(len(hosts))]
        dbs = [self.command_maker.path_maker.db_path(i) for i in range(len(hosts))]
        node_logs = [self.command_maker.path_maker.node_log_file(i) for i in range(len(hosts))]
        for host, key_file, db, log_file in zip(hosts, key_files, dbs, node_logs):
            cmd = self.command_maker.run_node(
                key_file,
                self.command_maker.path_maker.committee_file(),
                db,
                self.command_maker.path_maker.parameters_file(),
                debug=debug,
            )
            self._background_run(host, cmd, log_file)

        # Wait for the nodes to synchronize
        Print.info("Waiting for the nodes to synchronize...")
        sleep(2 * node_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f"Running benchmark ({duration} sec):"):
            sleep(ceil(duration / 20))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, hosts, faults):
        # Delete local logs (if any).
        cmd = self.local_command_maker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        progress = progress_bar(hosts, prefix="Downloading logs:")
        for i, host in enumerate(progress):
            # Start miniserve to download file via HTTP.
            logs_path = self.command_maker.path_maker.logs_path()
            cmd = self.command_maker.miniserve(logs_path, self.settings.miniserve_port)
            self._background_run(host, cmd, f'{logs_path}/miniserve.log')

            _download_file(
                host,
                self.settings.miniserve_port,
                logs_path,
                self.command_maker.path_maker.node_log_file(i),
                self.local_command_maker.path_maker.node_log_file(i)
            )
            _download_file(
                host,
                self.settings.miniserve_port,
                logs_path,
                self.command_maker.path_maker.client_log_file(i),
                self.local_command_maker.path_maker.client_log_file(i)
            )
        self.kill(hosts=hosts, delete_logs=False)

        # Parse logs and return the parser.
        Print.info("Parsing logs and computing performance...")
        return LogParser.process(self.local_command_maker.path_maker.logs_path(), faults=faults)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting remote benchmark")
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn("There are not enough instances available")
            return

        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError("Failed to update nodes", e)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                Print.heading(f"\nRunning {n} nodes (input rate: {r:,} tx/s)")
                hosts = selected_hosts[:n]

                # Upload all configuration files.
                try:
                    self._config(hosts, node_parameters)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError("Failed to configure nodes", e))
                    continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[: n - faults]

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f"Run {i+1}/{bench_parameters.runs}")
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, node_parameters, debug
                        )
                        self._logs(hosts, faults).print(
                            self.local_command_maker.path_maker.result_file(
                                faults, n, r, bench_parameters.tx_size
                            )
                        )
                    except (
                        subprocess.SubprocessError,
                        GroupException,
                        ParseError,
                    ) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError("Benchmark failed", e))
                        continue
