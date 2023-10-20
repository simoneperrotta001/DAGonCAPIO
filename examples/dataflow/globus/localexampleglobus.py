import json
import time
import os

from dagon import Workflow, DataMover
from dagon.task import DagonTask, TaskType

# Check if this is the main
if __name__ == '__main__':
    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Server")
    workflow.set_data_mover(DataMover.GRIDFTP)

    # Set the dry
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", "mkdir output;hostname > output/f1.txt",globusendpoint="b706f62a-6c56-11ee-b15f-7d6eafac2be9")

    # The task b
    taskB = DagonTask(TaskType.BATCH, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", globusendpoint="b706f62a-6c56-11ee-b15f-7d6eafac2be9")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    #workflow.add_task(taskC)
    #workflow.add_task(taskD)

    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()