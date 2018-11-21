from fabric.api import local, env, run, settings, hide


class DockerClient(object):
    def exec_command(self, command):
        with settings(
                hide('warnings', 'running', 'stdout', 'stderr'),
                warn_only=True
        ):
            res = local(command, capture=True)
            
            if len(res.stderr):
                return {"code": 1, "message": res.stderr}
            elif res.return_code != 0:
                return {"code": 1, "message": res.stdout}
            else:
                return {"code": 0, "message": res.stdout}

    @staticmethod
    def form_string_cont_creation(image, command=None, volume=None, ports=None, detach=False):
        docker_command = "docker run --net=\"host\""

        if detach:
            docker_command += " -t -d"

        if volume is not None:
            docker_command += " -v \'%s\':\'%s\'" % (volume['host'], volume['container'])
        if ports is not None:
            docker_command += " -p \'%s\':\'%s\'" % (ports['host'], ports['container'])
        docker_command += " %s" % image

        if command is not None:
            docker_command += " " + command

        return docker_command


class DockerRemoteClient(DockerClient):

    def __init__(self, ssh):
        self.ssh = ssh

    def exec_command(self, command):
        with settings(
                hide('warnings', 'running', 'stdout', 'stderr'),
                warn_only=True
        ):
            result = self.ssh.execute_command(command)
            return result
