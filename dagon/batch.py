import logging
from logging.config import fileConfig
import errno
import tempfile
from fabric.api import local, env
from fabric.context_managers import cd

import boto3
from paramiko import SSHClient
from communication.scp import SCPClient

from task import Task
from dagon import Workflow

class Batch(Task):

    env.use_ssh_config = True

    def __init__(self,name,command,working_dir=None):
        Task.__init__(self,name,command,working_dir)
        self.ip = "127.0.0.1" #by default runs on localhost

    def asJson(self):
        jsonTask=Task.asJson(self)
        jsonTask['command']=self.command
        return jsonTask

    def on_execute(self, command):
        # Execute the bash command
        result = local(command, capture=True)
        return result



class Slurm(Batch):

    def __init__(self,name,command,partition=None,ntasks=None,working_dir=None):
        Batch.__init__(self,name,command,working_dir)
        self.partition=partition
        self.ntasks=ntasks


    def on_execute(self, command):

        partition_text = ""
        if self.partition is not None:
            partition_text = "--partition=" + self.partition

        ntasks_text = ""
        if self.ntasks is not None:
            ntasks_text = "--ntasks=" + self.ntasks

        # Add the slurm batch command
        command = "sbatch " + partition_text + " " + ntasks_text + " --job-name " + self.name + " --chdir " + self.working_dir + " --output=" + self.name + "_output.txt --wait '" + command + "'"

        # Execute the bash command
        result = local(command, capture=True)
        return result

#class AWSEC2(Batch):
#
#  def __init__(self,name,image_id,machine_type,working_dir=None):
#    Batch.__init__(self,name,None)
#   self.image_id=image_id
#   self.machine_type=machine_type


