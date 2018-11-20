from dagon import Workflow
from dagon import batch
import json
import sys
import datetime
import os.path
import time

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/home/ccmmma/tmp/test6/",
    "remove_dir":False
  }

  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo",config)

  # Set the dry
  workflow.set_dry(False)
  
  # The task a
  taskA=batch.Slurm("A","mkdir output;hostname > output/f1.txt","hicpu",1)
  
  # The task b
  taskB=batch.Slurm("B","echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt","hicpu",1)
  
  # The task c
  taskC=batch.Slurm("C","echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt","hicpu",1)
  
  # The task d
  taskD=batch.Slurm("D","cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt","hicpu",1)
  
  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  workflow.make_dependencies()

  jsonWorkflow=workflow.asJson()
  with open('dataflow-demo-slurm.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()

  # Check if it is a dry run
  if workflow.get_dry() is False:
    # set the result filename
    result_filename = taskD.get_scratch_dir() + "/f3.txt"
    while not os.path.exists(result_filename):
      time.sleep(1)

    # get the results
    with open(result_filename, "r") as infile:
      result = infile.readlines()
      print result


