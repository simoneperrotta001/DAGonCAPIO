import time
import os
import tempfile
import shutil
from threading import Thread
from dagon import Workflow
from dagon import Stager
from fabric.api import local, env


from dagon import Status


class Task(Thread):

    def __init__(self, name, command="",working_dir=None):
        Thread.__init__(self)
        self.name = name
        self.nexts = []
        self.prevs = []
        self.reference_count = 0

        self.running = False
        self.workflow = None
        self.set_status(Status.READY)
        self.working_dir=working_dir
        self.command=command
        self.info=None


    def get_ip(self):
        return self.info["ip"]

    def get_user(self):
        return self.info["user"]

    def get_scratch_dir(self):
        while (self.working_dir==None):
            time.sleep(1)
        return self.working_dir

    def get_scratch_name(self):
        millis = int(round(time.time() * 1000))
        return str(millis) + "-" + self.name

    # asJson
    def asJson(self):
        jsonTask = {"name": self.name, "status": self.status.name,
                    "working_dir": self.working_dir, "nexts":[], "prevs":[]}
        for t in self.nexts:
            jsonTask['nexts'].append(t.name)
        for t in self.prevs:
            jsonTask['prevs'].append(t.name)
        return jsonTask

    # Set the workflow
    def set_workflow(self, workflow):
        self.workflow = workflow

    # Set the current status
    def set_status(self, status):
        self.status = status
        if self.workflow is not None:
            self.workflow.logger.debug("%s: %s", self.name, self.status)
            if self.workflow.regist_on_api:
                self.workflow.api.update_task_status(self.workflow.id, self.name, status.name)

    # Add the dependency to a task
    def add_dependency_to(self, task):
        task.nexts.append(self)
        self.prevs.append(task)

        if self.workflow.regist_on_api: #add in the server
            self.workflow.api.add_dependency(self.workflow.id, self.name, task.name)

    # By default asumes that is a local task
    def isTaskRemote(self):
        return False

    def isInOtherMachine(self, ip):
        return self.ip != ip

    # Increment the reference count
    def increment_reference_count(self):
        self.reference_count = self.reference_count + 1

    # Decremet the reference count
    def decrement_reference_count(self):
        self.reference_count = self.reference_count - 1

        # Check if the scratch directory must be removed
        if self.reference_count == 0 and self.remove_scratch_dir is True:
            # Remove the scratch directory
            # shutil.rmtree(self.working_dir)
            shutil.move(self.working_dir, self.working_dir + "-removed")

            # Perform some logging
            self.workflow.logger.debug("Removed %s", self.working_dir)

    # Method overrided
    def pre_run(self):
        # For each workflow:// in the command string
        ### Extract the referenced task
        ### Add a reference in the referenced task

        # Index of the starting position
        pos = 0

        # Forever unless no anymore Workflow.SCHEMA are present
        while True:
            # Get the position of the next Workflow.SCHEMA
            pos1 = self.command.find(Workflow.SCHEMA, pos)

            # Check if there is no Workflow.SCHEMA
            if pos1 == -1:
                # Exit the forever cycle
                break

            # Find the first occurrent of a whitespace (or if no occurrence means the end of the string)
            pos2 = self.command.find(" ", pos1)

            # Check if this is the last referenced argument
            if pos2 == -1:
                pos2 = len(self.command)

            # Extract the parameter string
            arg = self.command[pos1:pos2]

            # Remove the Workflow.SCHEMA label
            arg = arg.replace(Workflow.SCHEMA, "")

            # Split each argument in elements by the slash
            elements = arg.split("/")

            # Extract the referenced task's workflow name
            workflow_name = elements[0]

            # The task name is the first element
            task_name = elements[1]

            # Set the default workflow name if needed
            if workflow_name is None or workflow_name == "":
                workflow_name = self.workflow.name

            # Extract the reference task object
            # ToDo: manage the different workflow issue. Now it is not considered
            # change to something like
            #  task = self.workflow.find_task_by_name(workflow_name, task_name)
            task = self.workflow.find_task_by_name(task_name)

            # Check if the refernced task is consistent
            if task is not None:
                # Add the dependency to the task
                self.add_dependency_to(task)

                # Add the reference from the task
                task.increment_reference_count()

            # Go to the next element
            pos = pos2

    # Pre process command
    def pre_process_command(self, command):
        stager=Stager()

        # Initialize the script
        header="#! /bin/bash\n"
        header=header+"# This is the DagOn launcher script\n\n"

        # Add the howim script
        header=header+self.get_how_im_script()+"\n\n"

        # Create the header
        header = header+"# Change the current directory to the working directory\n"
        header = header+"cd " + self.working_dir + "\n\n"

        header = header + "# Start staging in\n\n"

        # Create the body
        body = command

        # Index of the starting position
        pos = 0

        # Forever unless no anymore Workflow.SCHEMA are present
        while True:
            # Get the position of the next Workflow.SCHEMA
            pos1 = command.find(Workflow.SCHEMA, pos)

            # Check if there is no Workflow.SCHEMA
            if pos1 == -1:
                # Exit the forever cycle
                break

            # Find the first occurrent of a whitespace (or if no occurrence means the end of the string)
            pos2 = command.find(" ", pos1)

            # Check if this is the last referenced argument
            if pos2 == -1:
                pos2 = len(command)

            # Extract the parameter string
            arg = command[pos1:pos2]

            # Remove the Workflow.SCHEMA label
            arg = arg.replace(Workflow.SCHEMA, "")

            # Split each argument in elements by the slash
            elements = arg.split("/")

            # Extract the referenced task's workflow name
            workflow_name = elements[0]

            # The task name is the first element
            task_name = elements[1]

            # Get the rest of the string as local path
            local_path = arg.replace(workflow_name + "/" + task_name, "")

            # Set the default workflow name if needed
            if workflow_name is None or workflow_name == "":
                workflow_name = self.workflow.name

            # Extract the reference task object
            # ToDo: manage the different workflow issue. Now it is not considered
            # change to something like
            #  task = self.workflow.find_task_by_name(workflow_name, task_name)
            task = self.workflow.find_task_by_name(task_name)

            # Check if the refernced task is consistent
            if task is not None:
                # Evaluate the destiation path
                dst_path = self.working_dir+"/.dagon/inputs/" + workflow_name + "/" + task_name

                # Create the destination directory
                header = header + "# Create the destination directory\n"
                header = header + "mkdir -p " + dst_path + "/" + os.path.dirname(local_path) + "\n\n"

                # ToDo: here the stager have to make the magic stuff
                #
                # if use link:
                #   create the link command
                #
                # if use cp
                #   ...
                #
                # if use_scp
                #   ...
                #
                # and so on
                #
                #

                # Add the move data command
                header=header+stager.stage_in(self,task,dst_path,local_path)

                # Change the body of the command
                body = body.replace(Workflow.SCHEMA + arg, dst_path + "/" + local_path)

            pos = pos2

        # Invoke the command
        header = header + "# Invoke the command\n"
        header = header + body + " |tee " + self.working_dir + "/.dagon/stdout.txt\n\n"
        return header

    # Post process the command
    def post_process_command(self, command):
        footer=command+"\n\n"
        footer=footer+"# Perform post process\n"
        return footer

    # Method to be overrided
    def on_execute(self, launcher_script):

        # The launcher script name
        launcher_script_name=self.working_dir + "/.dagon/launcher.sh"

        # Create a temporary launcher script
        file = open(launcher_script_name, "w")
        file.write(launcher_script)
        file.flush()
        file.close()
        os.chmod(launcher_script_name, 0744)

    # Method execute
    def execute(self):
        if self.working_dir is None:
            # Set a scratch directory as working directory
            self.working_dir = self.workflow.get_scratch_dir_base() + "/" + self.get_scratch_name()

            # Create scratch directory
            os.makedirs(self.working_dir+"/.dagon")

            # Set to remove the scratch directory
            self.remove_scratch_dir = True
        else:
            # Set to NOT remove the scratch directory
            self.remove_scratch_dir = False

        self.workflow.logger.debug("%s: Scratch directory: %s", self.name, self.working_dir)
        if self.workflow.regist_on_api:  # change scratch directory on server
            try:
                self.workflow.api.update_task(self.workflow.id, self.name, "working_dir", self.working_dir)
            except Exception, e:
                self.workflow.logger.error("%s: Error updating scratch directory on server %s", self.name, e)

        # Apply some command pre processing
        launcher_script = self.pre_process_command(self.command)

        # Apply some command post processing
        launcher_script = self.post_process_command(launcher_script)

        # Invoke the actual executor
        self.result =self.on_execute(launcher_script)


        # Check if the execution failed
        if self.result.failed:
            raise Exception('Executable raised a execption')

        # Remove the reference
        # For each workflow:// in the command

        # Index of the starting position
        pos = 0

        # Forever unless no anymore Workflow.SCHEMA are present
        while True:
            # Get the position of the next Workflow.SCHEMA
            pos1 = self.command.find(Workflow.SCHEMA, pos)

            # Check if there is no Workflow.SCHEMA
            if pos1 == -1:
                # Exit the forever cycle
                break

            # Find the first occurrent of a whitespace (or if no occurrence means the end of the string)
            pos2 = self.command.find(" ", pos1)

            # Check if this is the last referenced argument
            if pos2 == -1:
                pos2 = len(self.command)

            # Extract the parameter string
            arg = self.command[pos1:pos2]

            # Remove the Workflow.SCHEMA label
            arg = arg.replace(Workflow.SCHEMA, "")

            # Split each argument in elements by the slash
            elements = arg.split("/")

            # Extract the referenced task's workflow name
            workflow_name = elements[0]

            # The task name is the first element
            task_name = elements[1]

            # Set the default workflow name if needed
            if workflow_name is None or workflow_name == "":
                workflow_name = self.workflow.name

            # Extract the reference task object
            # ToDo: manage the different workflow issue. Now it is not considered
            # change to something like
            #  task = self.workflow.find_task_by_name(workflow_name, task_name)
            task = self.workflow.find_task_by_name(task_name)

            # Check if the refernced task is consistent
            if task is not None:
                # Remove the reference from the task
                task.decrement_reference_count()

            # Go to the next element
            pos = pos2

    def run(self):
        if self.workflow is not None:
            # Change the status
            self.set_status(Status.WAITING)

            # Wait for each previous tasks
            for task in self.prevs:
                task.join()

            # Check if one of the previous tasks crashed
            for task in self.prevs:
                if (task.status == Status.FAILED):
                    self.set_status(Status.FAILED)
                    return

            # Change the status
            self.set_status(Status.RUNNING)

            # Execute the task Job
            try:
                self.workflow.logger.debug("%s: Executing...", self.name)
                self.execute()
            except Exception, e:
                self.workflow.logger.error("%s: Except: %s", self.name, str(e))
                self.set_status(Status.FAILED)
                return
            #self.execute()

            # Start all next task
            for task in self.nexts:
                if (task.status == Status.READY):
                    self.workflow.logger.debug("%s: Starting task: %s", self.name, task.name)
                    try:
                        task.start()
                    except:
                        self.workflow.logger.warn("%s: Task %s already started.", self.name, task.name)

            # Change the status
            self.set_status(Status.FINISHED)
            return

    def get_how_im_script(self):
        return """
        
# Initialize
machine_type="none"
public_id="none"
user="none"
status_sshd="none"
status_ftpd="none"

if [ "$public_ip" == "" ]
then
  # Check if an AWS/EC2 machine
  LOCAL_HOSTNAME=$(hostname -d)
  if [[ ${LOCAL_HOSTNAME} =~ .*\.amazonaws\.com ]]
  then
    aws=`curl -s http://169.254.169.254/latest/meta-data/public-ipv4`
    if [ "$aws" != "" ]
    then
      machine_type="aws-ec2"
      public_ip=$aws
    fi
  fi
fi

if [ "$public_ip" == "" ]
then
  # The machine is a cluster frontend (or a single machine)
  machine_type="cluster-frontend"
  public_ip=`ifconfig 2>/dev/null| grep "inet "| grep -v "127.0.0.1"| awk '{print $2}'|grep -v "192.168."|grep -v "172.16."|grep -v "10."|head -n 1`
fi

if [ "$public_ip" == "" ]
then
  # If no public ip is available, then it is a cluster node
  machine_type="cluster-node"
  public_ip=`ifconfig 2>/dev/null| grep "inet "| grep -v "127.0.0.1"| awk '{print $2}'|head -n 1`  
fi


# Check if the secure copy is available
status_sshd=`service sshd status 2>/dev/null|grep "Active"| awk '{print $2}'`
if [ "$status_sshd" == "" ]
then
  status_sshd="none"
fi

# Check if the ftp is available
status_ftpd=`service vsftpd status 2>/dev/null|grep "Active"| awk '{print $2}'`
if [ "$status_ftpd" == "" ]
then
  status_ftpd="none"
fi

# Check if the grid ftp is available
status_gsiftpd=`service gsiftpd status 2>/dev/null|grep "Active"| awk '{print $2}'`
if [ "$status_gsiftpd" == "" ]
then
  status_gsiftpd="none"
fi

# Get the user
user=$USER

# Construct the json
json="{'type':'$machine_type','ip':'$public_ip','user':'$user','scp':'$status_sshd','ftp':'$status_ftpd','gsiftp':'$status_gsiftpd'}"

# Set the task info
#curl --header "Content-Type: application/json" --request POST --data "$json" http://"""+self.workflow.get_url()+"""/api/"""+self.name+"""/info
  """