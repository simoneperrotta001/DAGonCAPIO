from fabric.api import local, env, run, settings, hide

class DockerClient(object):
    def exec_command(self, command):
        with settings(
            hide('warnings', 'running', 'stdout', 'stderr'),
            warn_only=True
        ):
            res = local(command,capture=True)
            if not res.failed:
                return {"code":0, "output":res.stdout, "error":res.stderr} 
            else:
                return {"code":1, "output":res.stdout, "error":res.stderr} 

class DockerRemoteClient(DockerClient):

    def __init__(self, ssh):
        self.ssh = ssh

    def exec_command(self, command):
        with settings(
            hide('warnings', 'running', 'stdout', 'stderr'),
            warn_only=True
        ):
            result = self.ssh.executeCommand(command)
            return result