import os
import re
import shutil

from task import Task
from communication.ssh import SSHManager


class RemoteTask(Task):

    def __init__(self, name, ssh_username, keypath, command, ip=None, working_dir=None):
        Task.__init__(self, name, command, working_dir=working_dir)

        self.transfer = None
        self.ip = ip
        self.keypath = keypath
        self.ssh_username = ssh_username
        self.ssh_connection = None
        if self.ip is not None and self.ssh_username is not None:
            self.ssh_connection = SSHManager(self.ssh_username, self.ip, self.keypath)

    def on_execute(self, launcher_script, script_name):
        # The launcher script name
        script_name = self.working_dir + "/.dagon/" + script_name

        # Create a temporary launcher script
        self.ssh_connection.create_file(script_name, launcher_script)

    # make dir
    def mkdir_working_dir(self, path):
        res = SSHManager.execute_command(self.ssh_connection, "mkdir -p " + self.working_dir + "/.dagon")
        if res['code']:
            self.workflow.logger.error("%s: Error creating scratch directory on server %s", self.name)
            raise Exception('Cannot create scratch directory on remote')

    # remove scratch directory
    def on_garbage(self):
        SSHManager.execute_command(self.ssh_connection,
                                  'mv {0} {1}'.format(self.working_dir, self.working_dir + "-removed"))

