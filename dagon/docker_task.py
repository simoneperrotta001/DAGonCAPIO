from batch import Batch
from communication.data_transfer import DataTransfer
from dagon.communication.ssh import SSHManager
from dagon.remote import RemoteTask
from dockercontainer.container import Container
from dockercontainer.docker_client import DockerClient
from dockercontainer.docker_client import DockerRemoteClient
from task import Task


class LocalDockerTask(Batch):

    # Params:
    # 1) name: task name
    # 2) command: command to be executed
    # 3) image: docker image which the container is going to be created
    # 4) host: URL of the host, by default use the unix local host
    def __init__(self, name, command, container_id=None, working_dir=None, image=None, endpoint=None):
        Batch.__init__(self, name, command, working_dir=working_dir)
        self.command = command
        self.container_id = container_id
        self.working_dir = working_dir
        self.container = None
        self.image = image
        self.endpoint = endpoint
        self.docker_client = DockerClient()

    def as_json(self):
        json_task = Task.as_json(self)
        json_task['command'] = self.command
        return json_task

    # Create a Docker container
    def create_container(self):
        command = DockerClient.form_string_cont_creation(image=self.image, detach=True,
                                                         volume={"host": self.workflow.get_scratch_dir_base(),
                                                                 "container": self.workflow.get_scratch_dir_base()})
        result = self.docker_client.exec_command(command)
        if result['code']:
            raise Exception(self.result["message"].rstrip())
        return result['message']

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        Task.on_execute(self, launcher_script, script_name)
        self.container_id = self.create_container() if self.container_id is None else self.container_id
        self.container = Container(self.container_id, self.docker_client)
        return self.container.exec_in_cont(self.working_dir + "/.dagon/" + script_name)
        #return self.docker_client.exec_command(self.working_dir + "/.dagon/" + script_name)


class DockerRemoteTask(LocalDockerTask, RemoteTask):
    def __init__(self, name, command, image=None, container_id=None, ip=None, ssh_username=None, keypath=None,
                 working_dir=None):
        LocalDockerTask.__init__(self, name, command, container_id=container_id, working_dir=working_dir, image=image)
        RemoteTask.__init__(self, name=name, ssh_username=ssh_username, keypath=keypath, command=command, ip=ip,
                            working_dir=working_dir)

        self.ssh_connection = SSHManager(self.ssh_username, self.ip, self.keypath)
        self.docker_client = DockerRemoteClient(self.ssh_connection)

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        RemoteTask.on_execute(self, launcher_script, script_name)
        self.container_id = self.create_container().rstrip() if self.container_id is None else self.container_id
        self.container = Container(self.container_id, self.docker_client)
        return self.container.exec_in_cont(self.working_dir + "/.dagon/" + script_name)

class DockerTask(Task):

    def __init__(self, name, command, image=None, container_id=None, ip=None, port=None, ssh_username=None, keypath=None,
                 working_dir=None, endpoint=None):
        Task.__init__(self, name)

    def __new__(cls, name, command, image=None, container_id=None, ip=None, port=None, ssh_username=None, keypath=None,
                working_dir=None, local_working_dir=None, endpoint=None):
        is_remote = ip is not None
        if is_remote:
            pass
            return DockerRemoteTask(name, command, image=image, container_id=container_id, ip=ip,
                                    ssh_username=ssh_username, working_dir=working_dir,
                                    keypath=keypath)
        else:
            return LocalDockerTask(name, command, container_id=container_id, working_dir=working_dir, image=image)
