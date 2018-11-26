from dagon import Workflow
from dagon import batch
import json
import time
import os

# Check if this is the main
if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Server")

    # Set the dry
    workflow.set_dry(False)

    # The task a
    taskA = batch.Batch("A", "mkdir output;hostname > output/f1.txt", ip="disys0.tamps.cinvestav.mx", ssh_username="dsanchez")

    # The task b
    taskB = batch.Batch("B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", ip="disys0.tamps.cinvestav.mx", ssh_username="dsanchez")

    # The task c
    taskC = batch.Batch("C", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", ip="disys0.tamps.cinvestav.mx", ssh_username="dsanchez")

    # The task d
    taskD = batch.Batch("D", "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt", ip="disys0.tamps.cinvestav.mx", ssh_username="dsanchez")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()

    """if workflow.get_dry() is False:
        # set the result filename
        result_filename = taskD.get_scratch_dir() + "/f3.txt"
        while not os.path.exists(result_filename):
            time.sleep(1)

        # get the results
        with open(result_filename, "r") as infile:
            result = infile.readlines()
            print result"""
