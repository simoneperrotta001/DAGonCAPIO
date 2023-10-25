import json

from libcloud.compute.types import Provider

import dagon.cloud as cm
from dagon import Workflow
from dagon.remote import CloudTask

# Check if this is the main
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':

    # EC2 instance configuration
    ec2_flavour = {"image": "ami-0fc5d935ebf8bc3bc", "size": "t1.micro"}
    

    # The ssh key to access the EC2 instances
    #ssh_key_ec2_taskA = {"option": cm.KeyOptions.CREATE, "key_path": "dagon_services.pem", "cloud_args": {"name": "dagon_services_key"}} #uncomment to create a new key
    ssh_key_ec2_taskA = {"option": cm.KeyOptions.GET, "key_path": "dagon_services.pem", "cloud_args": {"name": "dagon_services"}}
    ssh_key_ec2_taskB = {"option": cm.KeyOptions.GET, "key_path": "dagon_services.pem", "cloud_args": {"name": "dagon_services_key"}}

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Cloud")

    # The task a
    taskA = DagonTask(TaskType.CLOUD, "A", "mkdir output;echo I am A > output/f1.txt", Provider.EC2, "ubuntu", ssh_key_ec2_taskA, instance_flavour=ec2_flavour, instance_name="dagonTaskA", stop_instance=True)

    # The task b (cloud)
    taskB = DagonTask(TaskType.CLOUD, "B", "echo $RANDOM > f2.txt; ls workflow:///A/output/f1.txt >> f2.txt", Provider.EC2, "ubuntu", ssh_key_ec2_taskB, instance_flavour=ec2_flavour, instance_name="dagonTaskB", stop_instance=True)

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-docker.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # run the workflow
    workflow.run()
