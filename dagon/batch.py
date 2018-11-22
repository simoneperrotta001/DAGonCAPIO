from fabric.api import local, env
from task import Task
from dagon.remote import RemoteTask


class Batch(Task):
    env.use_ssh_config = True

    def __init__(self, name, command, working_dir=None, ssh_username=None, keypath=None, ip=None):
        Task.__init__(self, name, command, working_dir)

    def __new__(cls, name, command, ssh_username=None, keypath=None, ip=None, working_dir=None):
        if ip is None:  # decide which type of batch task is it
            return super(Batch, cls).__new__(cls)  # return regular batch instance
        else:  # return remote batch instance
            return RemoteBatch(name=name, command=command, ssh_username=ssh_username, ip=ip, working_dir=working_dir,
                               keypath=keypath)

    def as_json(self):
        json_task = Task.as_json(self)
        json_task['command'] = self.command
        return json_task

    @staticmethod
    def execute_command(command):
        # Execute the bash command
        result = local(command, capture=True)
        # check for an error
        code, message = 0, ""
        if len(result.stderr):
            code, message = 1, result.stderr

        return {"code": code, "message": message}

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        super(Batch, self).on_execute(launcher_script, script_name)
        return Batch.execute_command(self.working_dir + "/.dagon/" + script_name)


class RemoteBatch(RemoteTask):

    def __init__(self, name, command, ssh_username=None, keypath=None, ip=None, working_dir=None):
        RemoteTask.__init__(self, name, ssh_username, keypath, command, ip=ip, working_dir=working_dir)

    def on_execute(self, launcher_script, script_name):
        # Invoke the base method
        RemoteTask.on_execute(self, launcher_script, script_name)
        result = self.ssh_connection.execute_command("bash " + self.working_dir + "/.dagon/" + script_name)
        return result


class Slurm:

    def __new__(cls, name, command, partition=None, ntasks=None, working_dir=None, ssh_username=None, keypath=None, ip=None):
        if ip is None:
            return LocalSlurm(name, command, partition, ntasks, working_dir)
        else:
            return RemoteSlurm(name, command, partition=partition, ntasks=ntasks, working_dir=working_dir,
                               ssh_username=ssh_username, ip=ip, keypath=keypath)


class LocalSlurm(Task):

    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None, ssh_username=None, keypath=None, ip=None):
        Task.__init__(self, name, command, working_dir)
        self.partition = partition
        self.ntasks = ntasks

    def generate_command(self, script_name):
        partition_text = ""
        if self.partition is not None:
            partition_text = "--partition=" + self.partition

        ntasks_text = ""
        if self.ntasks is not None:
            ntasks_text = "--ntasks=" + str(self.ntasks)

        # Add the slurm batch command
        # command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" + self.working_dir + " --output=" + self.working_dir + "/.dagon/stdout.txt --wait " + self.working_dir+"/.dagon/launcher.sh"
        command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" \
                  + self.working_dir + " --wait " + self.working_dir + "/.dagon/ " + script_name
        return command

    def on_execute(self, launcher_script, script_name):
        super(LocalSlurm, self).on_execute(launcher_script, script_name)

        if script_name == "context.sh":
            return Batch.execute_command(script_name)

        command = self.generate_command(script_name)

        # Execute the bash command
        result = Batch.execute_command(command)
        return result


class RemoteSlurm(RemoteTask, LocalSlurm):
    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None, ssh_username=None, keypath=None, ip=None):
        LocalSlurm.__init__(self, name, command, working_dir=working_dir, partition=partition, ntasks=ntasks)
        RemoteTask.__init__(self, name, ssh_username,keypath, command, ip, working_dir)

    def on_execute(self, launcher_script, script_name):
        RemoteTask.on_execute(self, launcher_script, script_name)

        if script_name == "context.sh":
            return self.ssh_connection.execute_command("bash " + self.working_dir + "/.dagon/" + script_name)

        command = self.generate_command(script_name)
        # Execute the bash command
        result =  self.ssh_connection.execute_command(command)
        return result

# class AWSEC2(Batch):
#
#  def __init__(self,name,image_id,machine_type,working_dir=None):
#    Batch.__init__(self,name,None)
#   self.image_id=image_id
#   self.machine_type=machine_type
