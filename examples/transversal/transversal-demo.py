
import json
import time
import os
import threading

"""
Transversality example
author: J.Armando Barron-lugo
date: 14/10/2023
description: 

    Build of a metaworkflow composed of 2 different workflows: DataFlow-Demo-Server and DataFlow-traversal.
    DataFlow traversal depends on DataFlow-Demo-Server tasks A and C.


"""
from dagon import Workflow
from dagon.dag_tps import DAG_TPS
from dagon.task import DagonTask, TaskType

# Check if this is the main
if __name__ == '__main__':

    config = {
        "scratch_dir_base": "/tmp/test6",
        "remove_dir": False
    }

    # Create the orchestration workflow
    workflow = Workflow("Transversal-Demo")

    # Set the dry
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", "mkdir output;echo 'A1,A2,A3' > output/f1.csv")

    # The task b
    taskB = DagonTask(TaskType.BATCH, "B", "echo 'B1,B2,B3' > f2.csv; cat workflow:///A/output/f1.csv >> f2.csv")

    # The task c
    taskC = DagonTask(TaskType.BATCH, "C", "echo 'C1,C2,C3' > f2.txt; cat workflow:///A/output/f1.csv >> f2.csv")

    # The task d
    taskD = DagonTask(TaskType.BATCH, "D", "cat workflow:///B/f2.csv >> f3.csv; cat workflow:///C/f2.csv >> f3.csv")

    # Second workflow
    workflow2 = Workflow("DataFlow-transversal")

    # Set the dry
    workflow2.set_dry(False)

    # The task E
    taskE = DagonTask(TaskType.BATCH, "E", "mkdir output;echo 'E1,E2,E3' > output/f1.csv")

    # The task f
    taskF = DagonTask(TaskType.BATCH, "F", "echo 'F1,F2,F3' > f2.csv; cat workflow://Transversal-Demo/A/output/f1.csv >> f2.csv; cat workflow:///E/output/f1.csv >> f2.csv")

    # The task g
    taskG = DagonTask(TaskType.BATCH, "G", "cat workflow:///F/f2.csv >> f3.csv; cat workflow://Transversal-Demo/C/f2.csv >> f3.csv")

    # Add tasks to the workflow 1
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    # Add tasks to the workflow 2
    workflow2.add_task(taskE)
    workflow2.add_task(taskF)
    workflow2.add_task(taskG)

    # List of the workflows
    metaworkflow = DAG_TPS("Transversal-Demo")
    metaworkflow.add_workflow(workflow)
    metaworkflow.add_workflow(workflow2)
    metaworkflow.make_dependencies()

    #
    jsonWorkflow = metaworkflow.as_json(json_format="mw") 
    with open('.MW-demo2.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # Run the meta workflow
    metaworkflow.run()