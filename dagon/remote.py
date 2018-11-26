from os.path import abspath

from communication.ssh import SSHManager
from dagon.cloud.cloud_manager import CloudManager
from task import Task


class RemoteTask(Task):

    def __init__(self, name, ssh_username, keypath, command, ip=None, working_dir=None):
        Task.__init__(self, name, command, working_dir=working_dir)
        self.transfer = None
        self.ip = ip
        self.keypath = abspath(keypath) if keypath is not None else keypath
        self.ssh_username = ssh_username
        self.ssh_connection = None
        if self.ip is not None and self.ssh_username is not None:
            self.ssh_connection = SSHManager(self.ssh_username, self.ip, self.keypath)

    def add_public_key(self, key):
        command = "echo " + key.strip() + "| cat >> ~/.ssh/authorized_keys"
        result = self.ssh_connection.execute_command(command)

    def on_execute(self, launcher_script, script_name):
        # The launcher script name
        script_name = self.working_dir + "/.dagon/" + script_name
        # Create a temporary launcher script
        self.ssh_connection.create_file(script_name, launcher_script)


    # make dir
    def mkdir_working_dir(self, path):
        res = SSHManager.execute_command(self.ssh_connection, "mkdir -p " + self.working_dir + "/.dagon")
        if res['code']:
            self.workflow.logger.error("%s: Error creating scratch directory on server %s", self.name, res['message'])
            raise Exception('Cannot create scratch directory on remote')

    # remove scratch directory
    def on_garbage(self):
        SSHManager.execute_command(self.ssh_connection,
                                   'mv {0} {1}'.format(self.working_dir, self.working_dir + "-removed"))

    def get_public_key(self):
        command = "cat " + self.working_dir + "/.dagon/ssh_key.pub"
        result = SSHManager.execute_command(self.ssh_connection, command)
        return result['output']


class CloudTask(RemoteTask):

    def __init__(self, name, command, provider, ssh_username, key_options, instance_id=None, instance_flavour=None, instance_name=None, stop_instance=False):
        super(CloudTask, self).__init__(name, ssh_username, key_options['key_path'], command)
        self.instance_id = instance_id
        self.provider = provider
        self.instance_flavour = instance_flavour
        self.key_options = key_options
        self.node = None
        self.stop_instance = stop_instance
        self.instance_name = instance_name

    def on_execute(self, launcher_script, script_name):
        RemoteTask.on_execute(self, launcher_script, script_name)
        return self.ssh_connection.execute_command("bash " + self.working_dir + "/.dagon/" + script_name)

    def execute(self):
        self.instance_name = self.instance_name if self.instance_name is not None else self.workflow.name.strip() + "-" + self.name
        self.node = CloudManager.getInstance(instance_id=self.instance_id, keyparams=self.key_options,
                                             flavour=self.instance_flavour, provider=self.provider,
                                             name=self.instance_name)
        self.ip = self.node.public_ips[0]
        self.ssh_connection = SSHManager(self.ssh_username, self.ip, self.keypath)
        super(CloudTask, self).execute()

    def on_garbage(self):
        RemoteTask.on_garbage(self)
        if self.stop_instance:
            self.ssh_connection.execute_command("shutdown -h now")
