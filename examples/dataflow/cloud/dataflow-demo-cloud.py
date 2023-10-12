import json

from libcloud.compute.types import Provider

import dagon.cloud as cm
from dagon import Workflow
from dagon.remote import CloudTask

# Check if this is the main
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':

    # EC2 instance configuration
    ec2_flavour = {"image": "ami-0bbe6b35405ecebdb",
                   "size": "t1.micro"
                   }
    # ssh_key_ec2 = {"option": cm.KeyOptions.CREATE, "key_path": "test-key.pem", "cloud_args": {"name": "test-key2"}}
    ssh_key_ec2 = {"option": cm.KeyOptions.GET, "key_path": "dagon_services.pem"}

    # Digital ocean configuration
    keysDO = cm.KeyPair.generate_RSA()
    ssh_key_do = {
        "option": cm.KeyOptions.CREATE,
        "key_path": "dagonDOkey.pem",
        "cloudargs": {
            "name": "new_key",
            "public_key": keysDO[1],
            "private_key": keysDO[0]
        }
    }
    do_flavour = {
        "image": "39769319",
        "size": "1gb",
        "location": "nyc1"
    }

    workflow = Workflow("DataFlow-Demo-Cloud")

    # The task a
    taskA = DagonTask(TaskType.CLOUD, "A", "mkdir output;echo I am A > output/f1.txt", Provider.EC2, "ubuntu", ssh_key_ec2,
                      instance_id="i-0792e2eeb013b0b2b", endpoint="880105d0-f2eb-11e8-8cc0-0a1d4c5c824a")

    # The task b (cloud)
    taskB = DagonTask(TaskType.CLOUD, "B", "echo $RANDOM > f2.txt; ls workflow:///A/output/f1.txt >> f2.txt", Provider.EC2,
                      "ubuntu", ssh_key_ec2, instance_id="i-0136ac7985609c759", endpoint="4ef4630c-f2f2-11e8-8cc0-0a1d4c5c824a")

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
