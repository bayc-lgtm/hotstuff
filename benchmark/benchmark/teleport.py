import subprocess, re
from paramiko import ProxyCommand

class TeleportConfig:
    port: int
    proxy_command: str  # includes quoted "/usr/local/bin/tsh" ... %r@%h:%p

    def __init__(self, port: int, proxy_command: str):
        self.port = port
        self.proxy_command = proxy_command

    def connect_kwargs(self, user:str, host: str) -> dict:
        proxy_command = self.proxy_command.replace("%r", user).replace("%h", host).replace("%p", str(self.port))
        return {
            "sock": ProxyCommand(proxy_command)
        }

def _run(cmd: list[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\nSTDERR:\n{p.stderr.strip()}")
    return p.stdout

def get_teleport_config() -> TeleportConfig:
    return _parse_tsh_config(_run(["tsh", "config"]))

def _parse_tsh_config(text: str) -> TeleportConfig:
    port = None
    proxy_command = None

    for line in text.splitlines():
        stripped = line.strip()
        m = re.match(r"^Port\s+(\d+)\s*$", stripped)
        if m:
            port = int(m.group(1))
            continue
        m = re.match(r'^ProxyCommand\s+(.+)$', stripped)
        if m:
            proxy_command = m.group(1)
            continue

    if port is None:
        raise ValueError("Could not find Port in `tsh config` output.")
    if proxy_command is None:
        raise ValueError("Could not find ProxyCommand in `tsh config` output.")
    if not proxy_command.endswith("%r@%h:%p"):
        raise ValueError("ProxyCommand does not end with %r@%h:%p")

    return TeleportConfig(port=port, proxy_command=proxy_command)
