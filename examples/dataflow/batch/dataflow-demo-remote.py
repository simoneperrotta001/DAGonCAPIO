from dagon import Workflow
from dagon import batch
import json
import time
import os

# Check if this is the main
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Remote")

    # Set the dry
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", "mkdir output;hostname > output/f1.txt", ip="", ssh_username="", keypath="")

    # The task b
    taskB = DagonTask(TaskType.BATCH, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", ip="", ssh_username="", keypath="")

    # The task c
    taskC = DagonTask(TaskType.BATCH, "C", "echo $RANDOM > f3.txt; cat workflow:///A/output/f1.txt >> f3.txt", ip="", ssh_username="", keypath="")

    # The task d
    taskD = DagonTask(TaskType.BATCH, "D", "cat workflow:///B/f2.txt >> f4.txt; cat workflow:///C/f3.txt >> f4.txt", ip="", ssh_username="", keypath="")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-remote.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()
