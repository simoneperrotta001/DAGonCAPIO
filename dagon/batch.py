from fabric.api import local, env
from task import Task


class Batch(Task):
    env.use_ssh_config = True

    def __init__(self, name, command, working_dir=None):
        Task.__init__(self, name, command, working_dir)

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


class Slurm(Task):

    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None):
        Task.__init__(self, name, command, working_dir)
        self.partition = partition
        self.ntasks = ntasks

    def on_execute(self, launcher_script, script_name):
        super(Slurm, self).on_execute(launcher_script, script_name)
        partition_text = ""
        if self.partition is not None:
            partition_text = "--partition=" + self.partition

        ntasks_text = ""
        if self.ntasks is not None:
            ntasks_text = "--ntasks=" + str(self.ntasks)

        # Add the slurm batch command
        # command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" + self.working_dir + " --output=" + self.working_dir + "/.dagon/stdout.txt --wait " + self.working_dir+"/.dagon/launcher.sh"
        command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" \
                  + self.working_dir + " --wait " + self.working_dir + "/.dagon/launcher.sh"

        # Execute the bash command
        result = Batch.execute_command(command)
        return result

# class AWSEC2(Batch):
#
#  def __init__(self,name,image_id,machine_type,working_dir=None):
#    Batch.__init__(self,name,None)
#   self.image_id=image_id
#   self.machine_type=machine_type
