from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:
    workspace: str
    sudo: bool
    path_maker: PathMaker

    def __init__(self, workspace, sudo):
        self.workspace = workspace
        self.sudo = sudo
        self.path_maker = PathMaker(workspace)

    def _sudo(self):
        return 'sudo' if self.sudo else ''

    def cleanup(self) -> list[str]:
        return (
            f'{self._sudo()} rm -r {self.workspace}/.db-* ; {self._sudo()} rm {self.workspace}/.*.json ; {self._sudo()} mkdir -p {self.path_maker.results_path()}'
        )

    def clean_logs(self):
        return f'{self._sudo()} rm -r {self.path_maker.logs_path()} ; {self._sudo()} mkdir -p {self.path_maker.logs_path()}'

    def _sudo_cargo_bin(self, bin: str) -> str:
        if self.sudo:
            bin = f'/root/.cargo/bin/{bin}'
        return f'{self._sudo()} {bin}'

    def compile(self):
        command = f'{self._sudo_cargo_bin("cargo")} build --quiet --release --features benchmark'
        if self.sudo:
            command += f' --manifest-path {self.workspace}/../Cargo.toml'
        return command

    def generate_key(self, filename):
        assert isinstance(filename, str)
        return f'{self._sudo()} {self.workspace}/node keys --filename {filename}'

    def run_node(self, keys, committee, store, parameters, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'{self._sudo()} {self.workspace}/node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters}')

    def run_client(self, address, size, rate, timeout, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'{self._sudo()} {self.workspace}/client {address} --size {size} '
                f'--rate {rate} --timeout {timeout} {nodes}')

    def kill(self):
        return f'{self._sudo()} tmux kill-server'

    def alias_binaries(self, origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'{self._sudo()} rm {self.workspace}/node ; {self._sudo()} rm {self.workspace}/client ; {self._sudo()} ln -s {node} {self.workspace} ; {self._sudo()} ln -s {client} {self.workspace}'

    def miniserve(self, path, port):
        return f'{self._sudo_cargo_bin("miniserve")} {path} --port {port}'
