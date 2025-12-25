from json import load, JSONDecodeError
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from benchmark.teleport import TeleportConfig, get_teleport_config
from benchmark.utils import BenchError

class SshAuth:
    type = 'ssh'
    key_name: str
    key_path: str
    user: str

    def __init__(self, key_name, key_path, user):
        self.key_name = key_name
        self.key_path = key_path
        self.user = user

    def connect_kwargs(self, _host: str) -> dict:
        try:
            pkey = RSAKey.from_private_key_file(
                self.key_path
            )
            return {"pkey": pkey}
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError("Failed to load SSH key", e)

class TeleportAuth:
    type = 'teleport'
    proxy: str
    user: str
    config: TeleportConfig

    def __init__(self, proxy, user):
        self.proxy = proxy
        self.user = user
        self.config = get_teleport_config()

    def connect_kwargs(self, host: str) -> dict:
        return self.config.connect_kwargs(self.user, host)

class AwsInstanceManagement:
    type = 'aws'
    instance_type: str
    aws_regions: list[str]

    def __init__(self, instance_type, aws_regions):
        self.instance_type = instance_type
        self.aws_regions = aws_regions

class TeleportInstanceManagement:
    type = 'teleport'
    hosts: list[str]

    def __init__(self, hosts):
        self.hosts = hosts

class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, testbed, auth: SshAuth | TeleportAuth, consensus_port, mempool_port,
                 front_port, miniserve_port, repo_name, repo_url, branch, instance: AwsInstanceManagement | TeleportInstanceManagement):
        self.testbed = testbed

        self.auth = auth

        self.consensus_port = consensus_port
        self.mempool_port = mempool_port
        self.front_port = front_port
        self.miniserve_port = miniserve_port

        self.repo_name = repo_name
        self.repo_url = repo_url
        self.branch = branch

        self.instance = instance

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            if 'teleport' in data:
                auth = TeleportAuth(
                    data['teleport']['proxy'],
                    data['teleport']['user'],
                )
                instance = TeleportInstanceManagement(
                    data['teleport']['hosts'],
                )
            else:
                auth = SshAuth(
                    data['key']['name'],
                    data['key']['path'],
                    'ubuntu',
                )
                instance = AwsInstanceManagement(
                    data['instances']['type'],
                    data['instances']['regions'],
                )

            return cls(
                data['testbed'],
                auth,
                data['ports']['consensus'],
                data['ports']['mempool'],
                data['ports']['front'],
                data['ports']['miniserve'],
                data['repo']['name'],
                data['repo']['url'],
                data['repo']['branch'],
                instance,
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
