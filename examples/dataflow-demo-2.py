
import sys
import json
import os.path
import datetime
from libcloud.compute.types import Provider

from dagon import batch
from dagon import Workflow
import dagon.cloud as cm
from dagon.remote import CloudTask
from dagon import docker_task as dt
from dagon.docker_task import DockerTask

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/test5",
    "remove_dir":False
  }
  
  digitalOceanFlavour = {"image":"38897365","size":"1gb","location":"nyc1"}
  keysshparams = {"option":cm.KeyOptions.CREATE,"keyname":"test-key","keypath":"test-key.pem","cloudargs":{"name":"test-key"}}
  keysshparams3 = {"option":cm.KeyOptions.CREATE,"keyname":"dagonkey","keypath":"dagonkey.pem","cloudargs":{"name":"dagonkey"}}
  keysDO = cm.KeyPair.generate_RSA()
  keysshparams2 = {"option":cm.KeyOptions.CREATE,"keypath":"dagonDOkey.pem",
                    "cloudargs":{"name":"dagonDOkey","public_key":keysDO[1],"private_key":keysDO[0]}}
  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo-Cloud",config)
  
  # The task a
  taskA=ct.CloudTask("A", "echo Soy A > f1.txt", Provider.EC2, keys,"ubuntu", id="i-03529037d63e2f3a8", create_instance=False, keyparams=keysshparams3,endpoint="40b6e558-e1a4-11e8-8c92-0a1d4c5c824a")

  taskB=dt.Docker("B","echo Soy B > f2.txt; cat workflow://A/f1.txt >> f2.txt", containerID="ad16ce696d9a",ip="18.236.141.12", ssh_username="ubuntu", keypath="test-key.pem", endpoint="aa44a782-dcf2-11e8-8c8d-0a1d4c5c824a")

  taskC=dt.Docker("C","echo $RANDOM > f2.txt; cat workflow://A/f1.txt >> f2.txt", image="ubuntu:18.04",endpoint="000288d8-dcf8-11e8-8c8d-0a1d4c5c824a", working_dir="test")
  

  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  #workflow.add_task(taskD)
  workflow.make_dependencies()

  jsonWorkflow=workflow.as_json()
  with open('dataflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()