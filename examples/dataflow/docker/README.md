# Docker Tasks with DagOnStar


DagOnStar supports the deployment of tasks on Docker containers.


## Requirements


* [Docker Engine](https://docs.docker.com/engine/install/ubuntu/)


## Pre-configurations


To run a task on a container, you first have to configure the base image with all the dependencies of the task.


A Docker task can be executed on both local and remote resources.


## Running Docker tasks on a local environment


The script ```dataflow-demo-docker.py``` contains an example of a workflow executed with Docker tasks (```TaskType.DOCKER```). Tasks can be executed on new containers or already deployed containers.


* To run a task on a new container, you must declare a task as follows:


```python
taskA = DagonTask(TaskType.DOCKER, "A", "mkdir output;hostname > output/f1.txt", image="ubuntu:20.04")
```


You only have to pass the name of the Docker image to build the virtual container.


* To run a task on an existing container, you must declare a task as follows:


```python
taskB = DagonTask(TaskType.DOCKER, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt", container_id="9eb6414f7e52")
```


You must pass the ID of your container. You can get it by executing on a terminal ```docker ps``` command to see running containers and their IDs.


```console
(venv) domizzi@domizzi:~/Documents/GitHub/dagonstar-globusdemo/examples/dataflow/docker$ docker ps
CONTAINER ID IMAGE COMMAND CREATED STATUS PORTS NAMES
b59ae968b286 centos:8 "/bin/bash" 41 minutes ago Up 41 minutes interesting_bouman
4c8a89b9b640 centos:8 "/bin/bash" 44 minutes ago Up 44 minutes peaceful_nobel
2c83b5ddc8ff centos:8 "/bin/bash" About an hour ago Up About an hour eloquent_ishizaka
9eb6414f7e52 ubuntu:20.04 "/bin/bash" About an hour ago Up About an hour admiring_jemison
(venv) domizzi@domizzi:~/Documents/GitHub/dagonstar-globusdemo/examples/dataflow/docker$
```


> [!WARNING]
> The containers will be deleted by the garbage collector when the execution of a task is completed and it does not have more references. To avoid this, add ```remove=False``` as a parameter of a task.


### Execution of the demo


Open the root directory of DagOnStar in a terminal, and run the following commands to prepare it.


```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD:$PYTHONPATH
```


Now navigate to the directory of the demo.


```bash
cd examples/dataflow/docker
```


Execute the file ```dataflow-demo-cloud.py``` as follows:


```bash
python dataflow-demo-docker.py
```


During the execution of the workflows, the containers of the tasks will be created. When the execution of the workflow is completed, you must see an output on the terminal as follows:


(venv) domizzi@domizzi:~/Documents/GitHub/dagonstar-globusdemo/examples/dataflow/docker$ python3 dataflow-demo-docker.py
2023-10-26 13:31:09,155 root DEBUG Running workflow: DataFlow-Demo-Docker
2023-10-26 13:31:09,155 root DEBUG A: Status.WAITING
2023-10-26 13:31:09,155 root DEBUG A: Status.RUNNING
2023-10-26 13:31:09,155 root DEBUG A: Executing...
2023-10-26 13:31:09,156 root DEBUG B: Status.WAITING
2023-10-26 13:31:09,156 root DEBUG C: Status.WAITING
2023-10-26 13:31:09,156 root DEBUG D: Status.WAITING
2023-10-26 13:31:09,156 root DEBUG A: Scratch directory: /tmp//1698319869156-A
2023-10-26 13:31:10,073 root INFO A: Successfully pulled ubuntu:20.04
2023-10-26 13:31:11,019 root DEBUG A Completed in 0.1872119903564453 seconds ---
2023-10-26 13:31:13,021 root DEBUG A: Status.FINISHED
2023-10-26 13:31:13,021 root DEBUG B: Status.RUNNING
2023-10-26 13:31:13,021 root DEBUG C: Status.RUNNING
2023-10-26 13:31:13,021 root DEBUG B: Executing...
2023-10-26 13:31:13,022 root DEBUG C: Executing...
2023-10-26 13:31:13,022 root DEBUG B: Scratch directory: /tmp//1698319873022-B
2023-10-26 13:31:13,022 root DEBUG C: Scratch directory: /tmp//1698319873022-C
2023-10-26 13:31:13,811 root DEBUG B Completed in 0.1349623203277588 seconds ---
2023-10-26 13:31:14,959 root INFO C: Successfully pulled python:3.9
2023-10-26 13:31:15,606 root DEBUG C Completed in 0.13543200492858887 seconds ---
2023-10-26 13:31:15,693 root DEBUG Removed /tmp//1698319869156-A
2023-10-26 13:31:15,813 root DEBUG B: Status.FINISHED
2023-10-26 13:31:17,694 root DEBUG C: Status.FINISHED
2023-10-26 13:31:17,694 root DEBUG D: Status.RUNNING
2023-10-26 13:31:17,694 root DEBUG D: Executing...
2023-10-26 13:31:17,695 root DEBUG D: Scratch directory: /tmp//1698319877695-D
2023-10-26 13:31:18,564 root INFO D: Successfully pulled centos:8
2023-10-26 13:31:19,766 root DEBUG D Completed in 0.1727442741394043 seconds ---
2023-10-26 13:31:19,858 root DEBUG Removed /tmp//1698319873022-B
2023-10-26 13:31:29,985 root DEBUG Removed /tmp//1698319873022-C
2023-10-26 13:31:31,986 root DEBUG D: Status.FINISHED
2023-10-26 13:31:31,986 root INFO Workflow 'DataFlow-Demo-Docker' completed in 22.831312656402588 seconds ---
['17935\n', 'domizzi\n', '24719\n', 'domizzi\n']
(venv) domizzi@domizzi:~/Documents/GitHub/dagonstar-globusdemo/examples/dataflow/docker$
```


> [!NOTE]
> By default, each containers is deployed with a volume to the scratch directory of the tasks. So, you can access this directory to see the results of a task.

