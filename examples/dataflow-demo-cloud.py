import sys
import json
import os.path
import datetime
from libcloud.compute.types import Provider

from dagon import batch, Slurm
from dagon import Workflow
from dagon.docker_task import DockerTask
from dagon.remote import CloudTask
from dagon import docker_task as dt
import dagon.cloud.cloud_manager as cm

# Check if this is the main
if __name__ == '__main__':

    # EC2 instance configuration
    ec2_flavour = {"image": "ami-0bbe6b35405ecebdb", "size": "t1.micro"}
    # ssh_key_ec2 = {"option": cm.KeyOptions.CREATE, "key_path": "test-key.pem", "cloud_args": {"name": "test-key2"}}
    ssh_key_ec2 = {"option": cm.KeyOptions.GET, "key_path": "test-key.pem", "cloud_args": {"name": "test-key2"}}

    # Digital ocean configuration
    keysDO = cm.KeyPair.generate_RSA()
    ssh_key_do = {"option": cm.KeyOptions.CREATE, "key_path": "dagonDOkey.pem", "cloudargs": {"name": "new_key", "public_key": keysDO[1], "private_key": keysDO[0]}}
    do_flavour = {"image": "39769319", "size": "1gb", "location": "nyc1"}

    workflow = Workflow("DataFlow-Demo-Cloud")

    # The task a
    taskA = CloudTask("A", "mkdir output;echo I am A > output/f1.txt", Provider.EC2, "ubuntu", ssh_key_ec2,
                      instance_id="i-0373496c6289cd908")

    # The task a (batch)
    #taskA = batch.Batch("A", "mkdir output;hostname > output/f1.txt")

    # The task b (cloud)
    taskB = CloudTask("B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", Provider.DIGITAL_OCEAN,
                      "root", ssh_key_do, instance_name="DataFlow-Demo-Cloud-B")

    # The task b (slurm)
    taskC = Slurm("C", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", "testing", 1,
                  ip="159.89.8.253", ssh_username="batman")

    taskD = DockerTask("D", "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt", image="ubuntu_curl",
                       ip="ec2-34-219-189-203.us-west-2.compute.amazonaws.com", ssh_username="ubuntu", keypath="dagon_services.pem")

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)
    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-docker.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()
