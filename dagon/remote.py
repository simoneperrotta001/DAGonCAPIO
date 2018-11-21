import os
import re
import shutil

from batch import Batch
from communication.ssh import SSHManager


class RemoteTask(Batch):

    def __init__(self, name, ssh_username, keypath, command, ip=None, working_dir=None):
        Batch.__init__(self, name, command, working_dir=working_dir)

        self.transfer = None
        self.ip = ip
        self.keypath = keypath
        self.command = command
        self.working_dir = working_dir
        self.ssh_username = ssh_username
        self.ssh_connection = None

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
    def rm_scratch_directory(self):
        SSHManager.execute_command(self.ssh_connection,
                                  'mv {0} {1}'.format(self.working_dir, self.working_dir + "-removed"))

