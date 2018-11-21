import paramiko
from fabric.api import local
from paramiko import SSHClient

from connection import Connection


# To manage SSH connections
class SSHManager:

    def __init__(self, username, host, keypath):
        self.username = username
        self.host = host
        self.keypath = keypath
        self.connection = self.get_ssh_connection()

    def get_connection(self):
        return self.connection

    # add host to know hosts
    @staticmethod
    def add_to_know_hosts(node):
        command = "ssh-keyscan -H %s >> ~/.ssh/known_hosts" % (node)
        result = local(command, capture=False, shell="/bin/bash")
        # Check if the execution failed    
        if result.failed:
            raise Exception('Failed to add to know hosts')

    def create_file(self, file, content):
        sftp = self.get_connection().open_sftp()
        f = sftp.open(file, 'w')
        f.write(content)
        f.close()

    # Return a SSH connection
    def get_ssh_connection(self):
        # SSHManager.addToKnowHosts(self.host) #add to know hosts
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        while not Connection.isPortOpen(self.host, 22):
            continue
        if self.keypath is None:
            ssh.connect(self.host, username=self.username)
        else:
            ssh.connect(self.host, username=self.username, key_filename=self.keypath)
        return ssh

    # execute command in remothe machine over SSH
    def execute_command(self, command):
        _, stdout, stderr = self.connection.exec_command(command)
        code = stdout.channel.recv_exit_status()
        stdout = "\n".join(stdout.readlines())
        stderr = "\n".join(stderr.readlines())

        if len(stderr):
            return {"code": 1, "message": stderr}
        elif code > 0:
            return {"code": 1, "message": stdout}
        else:
            return {"code": 0, "message": stdout}