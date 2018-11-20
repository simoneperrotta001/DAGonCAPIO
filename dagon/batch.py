from fabric.api import local, env
from task import Task


class Batch(Task):
    env.use_ssh_config = True

    def __init__(self, name, command, working_dir=None):
        Task.__init__(self, name, command, working_dir)
        self.ip = "127.0.0.1"  # by default runs on localhost

    def asJson(self):
        jsonTask = Task.asJson(self)
        jsonTask['command'] = self.command
        return jsonTask

    def on_execute(self, launcher_script):
        # Invoke the base method
        super(Batch, self).on_execute(launcher_script)
        # Execute the bash command
        result = local(self.working_dir + "/.dagon/launcher.sh", capture=True)
        # check for an error
        code, message = 0, ""
        if len(result.stderr):
            code, message = 1, result.stderr

        return {"code": code, "message": message}


class Slurm(Task):

    def __init__(self, name, command, partition=None, ntasks=None, working_dir=None):
        Task.__init__(self, name, command, working_dir)
        self.partition = partition
        self.ntasks = ntasks

    def on_execute(self, launcher_script):
        super(Slurm, self).on_execute(launcher_script)
        partition_text = ""
        if self.partition is not None:
            partition_text = "--partition=" + self.partition

        ntasks_text = ""
        if self.ntasks is not None:
            ntasks_text = "--ntasks=" + str(self.ntasks)

        # Add the slurm batch command
        # command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" + self.working_dir + " --output=" + self.working_dir + "/.dagon/stdout.txt --wait " + self.working_dir+"/.dagon/launcher.sh"
        command = "sbatch " + partition_text + " " + ntasks_text + " --job-name=" + self.name + " --chdir=" + self.working_dir + " --wait " + self.working_dir + "/.dagon/launcher.sh"

        # Execute the bash command
        result = local(command, capture=False)
        return result

# class AWSEC2(Batch):
#
#  def __init__(self,name,image_id,machine_type,working_dir=None):
#    Batch.__init__(self,name,None)
#   self.image_id=image_id
#   self.machine_type=machine_type
