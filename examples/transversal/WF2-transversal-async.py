
import json
import time
import os
import threading

"""
Asynchronous dependency
author: J.Armando Barron-lugo
date: 14/10/2023
description: 

    Example of an asynchronous traversal dependency. This script describes a generic workflow in Dagon but with a dependency to wf1-transversal-demo in 
    WF1-traversal-async.py. 

    Once tasks A and C of the wf1-transversal-demo workflow are completed, the wf2-transversal-demo workflow will continue its execution.
    This example requires DagOnService to work.
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


#second workflow
    workflow2 = Workflow("wf2-transversal-demo")
    workflow2.set_dry(False)
    # The task E
    taskE = DagonTask(TaskType.BATCH, "E", "mkdir output;hostname > output/f1.txt")

    # The task f
    taskF = DagonTask(TaskType.BATCH, "F", "echo $RANDOM > f2.txt; cat workflow://wf1-transversal-demo/A/output/f1.txt >> f2.txt; cat workflow:///E/output/f1.txt >> f2.txt")

    # The task g
    taskG = DagonTask(TaskType.BATCH, "G", "cat workflow:///F/f2.txt >> f3.txt; cat workflow://wf1-transversal-demo/C/f2.txt >> f3.txt")

    # add tasks to the workflow 2
    workflow2.add_task(taskE)
    workflow2.add_task(taskF)
    workflow2.add_task(taskG)
    workflow2.make_dependencies()

    jsonWorkflow = workflow2.as_json() 
    with open('wf2-transversal-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)
    # run the workflow
    workflow2.run()
