from fabric.api import local, env
from fabric.context_managers import settings, hide

from task import Task
from dagon.remote import RemoteTask


class Batch(Task):
    env.use_ssh_config = True

    def __init__(self, name, command, working_dir=None):
        """Create an Local Batch class

           Keyword arguments:
           name -- task name
           command -- command to be executed
           working_dir -- directory where the outputs will be placed
        """
        Task.__init__(self, name, command, working_dir)

    def __new__(cls, *args, **kwargs):
        """Create an Batch task local or remote

           Keyword arguments:
           name -- task name
           command -- command to be executed
           working_dir -- directory where the outputs will be placed
           ip -- hostname or ip of the machine where the task will be executed
           ssh_username -- username in remote machine
           keypath -- path to the private keypath
        """
        if "ip" in kwargs:
            return super(Batch, cls).__new__(RemoteBatch)
        else:
            return super(Batch, cls).__new__(cls)

    def as_json(self):
        json_task = Task.as_json(self)
        json_task['command'] = self.command
        return json_task

    @staticmethod
    def execute_command(command):
        # Execute the bash command
        with settings(
                hide('warnings', 'running', 'stdout', 'stderr'),
                warn_only=True
        ):
            result = local(command, capture=True)
            # check for an error
            code, message = 0, ""
            if len(result.stderr):
                code, message = 1, result.stderr
            return {"code": code, "message": message, "output":result.stdout}

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        super(Batch, self).on_execute(launcher_script, script_name)
        return Batch.execute_command(self.working_dir + "/.dagon/" + script_name)

    # returns public key
    def get_public_key(self):
        command = "cat " + self.working_dir + "/.dagon/ssh_key.pub"
        result = Batch.execute_command(command)
        return result['output']

    def add_public_key(self, key):
        command = "echo " + key.strip() + "| cat >> ~/.ssh/authorized_keys"
        result = Batch.execute_command(command)

class RemoteBatch(RemoteTask, Batch):

    def __init__(self, name, command, ssh_username=None, keypath=None, ip=None, working_dir=None):
        """Create an Batch task remote

           Keyword arguments:
           name -- task name
           command -- command to be executed
           working_dir -- directory where the outputs will be placed
           ip -- hostname or ip of the machine where the task will be executed
           ssh_username -- username in remote machine
           keypath -- path to the private keypath
        """
        RemoteTask.__init__(self, name, ssh_username, keypath, command, ip=ip, working_dir=working_dir)

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        RemoteTask.on_execute(self, launcher_script, script_name)
        result = self.ssh_connection.execute_command("bash " + self.working_dir + "/.dagon/" + script_name)
        return result


class Slurm(Batch):

    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None):
        Batch.__init__(self, name, command, working_dir)
        self.partition = partition
        self.ntasks = ntasks

    def __new__(cls, *args, **kwargs):
        """Create an Slurm task local or remote

           Keyword arguments:
           name -- task name
           command -- command to be executed
           partition -- partition where the task is going to be executed
           ntasks -- number of tasks to execute
           working_dir -- directory where the outputs will be placed
        """
        if "ip" in kwargs:
            return super(Task, cls).__new__(RemoteSlurm)
        else:
            return super(Slurm, cls).__new__(cls)

    def generate_command(self, script_name):
        partition_text = ""
        if self.partition is not None:
            partition_text = "--partition=" + self.partition

        ntasks_text = ""
        if self.ntasks is not None:
            ntasks_text = "--ntasks=" + str(self.ntasks)

        # Add the slurm batch command
        # command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" + self.working_dir + " --output=" + self.working_dir + "/.dagon/stdout.txt --wait " + self.working_dir+"/.dagon/launcher.sh"
        command = "sbatch " + partition_text + " " + ntasks_text + " -J " + self.name + " -D " \
                  + self.working_dir + " -W " + self.working_dir + "/.dagon/" + script_name
        return command

    def on_execute(self, launcher_script, script_name):
        super(Batch, self).on_execute(launcher_script, script_name)

        if script_name == "context.sh":
            return Batch.execute_command(self.working_dir + "/.dagon/" + script_name)

        command = self.generate_command(script_name)

        # Execute the bash command
        result = Batch.execute_command(command)
        return result


class RemoteSlurm(RemoteTask, Slurm):
    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None, ssh_username=None, keypath=None,
                 ip=None):
        Slurm.__init__(self, name, command, working_dir=working_dir, partition=partition, ntasks=ntasks)
        RemoteTask.__init__(self, name, ssh_username, keypath, command, ip, working_dir)

    def on_execute(self, launcher_script, script_name):
        RemoteTask.on_execute(self, launcher_script, script_name)
        if script_name == "context.sh":
            return self.ssh_connection.execute_command("bash " + self.working_dir + "/.dagon/" + script_name)

        command = self.generate_command(script_name)
        # Execute the bash command
        result = self.ssh_connection.execute_command(command)
        return result

# class AWSEC2(Batch):
#
#  def __init__(self,name,image_id,machine_type,working_dir=None):
#    Batch.__init__(self,name,None)
#   self.image_id=image_id
#   self.machine_type=machine_type
