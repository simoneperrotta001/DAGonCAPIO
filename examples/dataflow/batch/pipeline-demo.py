import json
import time
import os
from time import sleep

from dagon import Workflow
from dagon.task import DagonTask, TaskType


# Check if this is the main
if __name__ == '__main__':
    # Create the orchestration workflow
    workflow = Workflow("Pipeline-Demo")

    # Set the dry, se è falsa allora l'esecuzione avverrà effettivamente
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", "/home/s.perrotta/capio/build/src/A")

    # The task b
    taskB = DagonTask(TaskType.BATCH, "B", "/home/s.perrotta/capio/build/src/B workflow:///A")

    taskC = DagonTask(TaskType.BATCH, "C", "/home/s.perrotta/capio/build/src/C workflow:///B")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)

    workflow.make_dependencies()
    workflow.create_scratch_directory_names_tasks_capio()

    jsonCapioWorkflow = workflow.as_json_capio()
    with open('pipeline-demo-capio.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonCapioWorkflow, sort_keys=False, indent=2)
        outfile.write(stringWorkflow)

    workflow.set_capio_server_path("/home/s.perrotta/capio_new/build_release/src/server")
    workflow.set_capio_libcapioposix_path("/home/s.perrotta/capio_new/build_release/src/posix")
    workflow.run_capio_server()
    sleep(2)

    workflow.create_scratch_directory_tasks_capio()
    sleep(5)

    jsonWorkflow = workflow.as_json()
    with open('pipeline-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    workflow.generate_script_pipeline()
    workflow.remove_all_task_reference_workflow()

    sleep(10)