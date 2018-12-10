
import json
from libcloud.compute.types import Provider

from dagon import Workflow
from dagon.remote import CloudTask
from dagon.docker_task import DockerTask
from dagon.cloud import KeyPair
from dagon.cloud import KeyOptions

# Check if this is the main
if __name__ == '__main__':

  config={
    "scratch_dir_base":"/tmp/test6/",
    "remove_dir":False
  }

  #Images configuration
  defaultflavour = {"image":"ami-0bbe6b35405ecebdb","size":"t1.micro"} #EC2
  digitalOceanFlavour = {"image":"38897365","size":"1gb","location":"nyc1"} #Digital ocean
  googleFlavour = {"image":'debian-7',"size":"n1-standard-1","location":"us-east1-b"} #google

  keysshparams = {"option":KeyOptions.GET,"keyname":"test-key","keypath":"test-key.pem","cloudargs":{"name":"test-key"}}
  keysDO =  KeyPair.generate_RSA()

  googleKeyParams = {"keypath":"googlekey.pem","username":"dagon","public_key":keysDO[1],"private_key":keysDO[0]}

  keysshparams3 = {"option":KeyOptions.GET,"keyname":"dagonkey","keypath":"dagonkey.pem","cloudargs":{"name":"dagonkey"}}
  
  keysshparams2 = {"option":KeyOptions.CREATE,"keypath":"dagonDOkey.pem",
                    "cloudargs":{"name":"dagonDOkey","public_key":keysDO[1],"private_key":keysDO[0]}}
  # Create the orchestration workflow
  workflow=Workflow("DataFlow-Demo-Cloud",config)

  # The task a: a EC2 Instance
  taskA= CloudTask("A", "mkdir output;echo I am A > output/f1.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-021d09715bc0b877c", keyparams=keysshparams3,endpoint="6c24aea0-e208-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="dagon")

  # The task B: another EC2 instance
  taskB= CloudTask("B", "echo I am B > f2.txt; cat workflow://A/output/f1.txt >> f2.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-03d1c4f2b326ed016", keyparams=keysshparams3,flavour=defaultflavour, instance_name="B")

  # The task C: a Docker local container 
  taskC=DockerTask("C","mkdir output;ls > output/f4.txt;echo I am C > f3.txt; cat workflow://A/output/f1.txt >> f3.txt", image="ubuntu:18.04")

  # Task D: another EC2 instance
  taskD= CloudTask("D", "echo I am D > f1.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-021d09715bc0b877c", keyparams=keysshparams3,endpoint="6c24aea0-e208-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="dagon")

  #Task E: Another EC2 instance
  taskE= CloudTask("E", "echo I am E > f4.txt; cat workflow://D/f1.txt >> f4.txt; cat workflow://C/f3.txt >> f4.txt", Provider.EC2,"ubuntu",create_instance=False,id="i-021d09715bc0b877c", keyparams=keysshparams3,endpoint="6c24aea0-e208-11e8-8c92-0a1d4c5c824a",flavour=defaultflavour, instance_name="dagon")
  
  # The task F: a Docker local container
  taskF=DockerTask("F","echo I am F > f7.txt; cat workflow://C/f3.txt >> f7.txt; cat workflow://C/output/f4.txt >> "
                       "f7.txt ", image="ubuntu:18.04")

  # Task G: A Google Compute instance
  #taskG = CloudTask("G", "I am on Google > f8.txt; cat workflow://F/f7.txt >> f8.txt;", Provider.GCE, "dagon", create_instance=False, keyparams=googleKeyParams,flavour=googleFlavour, instance_name="dagontest2")
  

  # add tasks to the workflow
  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)
  workflow.add_task(taskE)
  workflow.add_task(taskF)
  #workflow.add_task(taskG)
  workflow.make_dependencies()
  #workflow.draw()
  jsonWorkflow=workflow.as_json()

  with open('dataflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
 
  # run the workflow
  workflow.run()