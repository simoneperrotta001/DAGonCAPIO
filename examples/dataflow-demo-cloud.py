
import sys
import json
import os.path
import datetime
from libcloud.compute.types import Provider

from dagon import batch
from dagon import Workflow
from dagon.remote import CloudTask
from dagon import docker_task as dt
import dagon.cloud.cloud_manager as cm

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/test6/",
    "remove_dir":False
  }

  defaultflavour = {"image":"ami-0bbe6b35405ecebdb","size":"t1.micro"}
  digitalOceanFlavour = {"image":"38897365","size":"1gb","location":"nyc1"}
  keysshparams = {"option":cm.KeyOptions.GET,"keyname":"test-key","keypath":"test-key.pem","cloudargs":{"name":"test-key"}}
  keysshparams3 = {"option":cm.KeyOptions.GET,"keyname":"dagonkey","keypath":"dagonkey.pem","cloudargs":{"name":"dagonkey"}}
  keysDO = cm.KeyPair.generate_RSA()
  keysshparams2 = {"option":cm.KeyOptions.CREATE,"keypath":"dagonDOkey.pem",
                    "cloudargs":{"name":"dagonDOkey","public_key":keysDO[1],"private_key":keysDO[0]}}
  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo-Cloud",config)
  
  # The task a
  taskA= CloudTask("A", "mkdir output;echo I am A > output/f1.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-021d09715bc0b877c", keyparams=keysshparams3,endpoint="6c24aea0-e208-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="dagon")

  taskB= CloudTask("B", "echo I am B > f2.txt; cat workflow://A/output/f1.txt >> f2.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-03d1c4f2b326ed016", keyparams=keysshparams3,endpoint="fc4e86e0-e203-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="B")

  taskC= CloudTask("C", "echo I am C > f2.txt; cat workflow://A/output/f1.txt >> f2.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-03d1c4f2b326ed016", keyparams=keysshparams3,endpoint="fc4e86e0-e203-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="C")
  

  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.make_dependencies()

  jsonWorkflow=workflow.asJson()
  with open('dataflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()