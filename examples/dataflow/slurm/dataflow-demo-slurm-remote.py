from dagon import Workflow
from dagon.task import DagonTask, TaskType
import json

import os.path
import time

# Check if this is the main
if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Slurm-Remote")

    # Set the dry
    workflow.set_dry(False)

    # The task A
    taskA = DagonTask(TaskType.SLURM, "A", "mkdir output;hostname > output/f1.txt", partition="", ntasks=1, memory=8192, ip="", ssh_username="")

    # The task B
    taskB = DagonTask(TaskType.SLURM, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", partition="", ntasks=1, memory=8192, ip="", ssh_username="")

    # The task C
    taskC = DagonTask(TaskType.SLURM, "C", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", partition="", ntasks=1, memory=8192, ssh_username="")

    # The task D
    taskD = DagonTask(TaskType.SLURM, "D", "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt", partition="", ntasks=1, memory=8192, ssh_username="")

    # add the task A to the workflow
    workflow.add_task(taskA)

    # add the task B to the workflow
    workflow.add_task(taskB)

    # add the task C to the workflow
    workflow.add_task(taskC)

    # add the task D to the workflow
    workflow.add_task(taskD)

    # Resolve the dependencies
    workflow.make_dependencies()

    # Create a json dump of the workflow
    jsonWorkflow = workflow.as_json()

    # Create a text file to save the json
    with open('dataflow-demo-slurm.json', 'w') as outfile:

        # Get a string representation of the json object
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)

        # Write the json workflow
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()

    # Check if it is a dry run
    if workflow.get_dry() is False:

        # set the result filename
        result_filename = taskD.get_scratch_dir() + "/f3.txt"

        # Wait until the output file is created
        while not os.path.exists(result_filename):

            # Until the file is not already created, sleep for a second
            time.sleep(1)

        # get the results
        with open(result_filename, "r") as infile:

            # Read the file lines
            result = infile.readlines()

            # Print the result on the console
            print(result)

