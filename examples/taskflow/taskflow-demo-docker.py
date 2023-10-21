import json
from dagon import Workflow
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':
  workflow = Workflow("Taskflow-Demo-Docker")

  taskA = DagonTask(TaskType.DOCKER, "Tokio", "/bin/hostname >tokio.out", image="ubuntu:latest")
  taskB = DagonTask(TaskType.DOCKER, "Berlin","/bin/date", image="ubuntu:latest")
  taskC = DagonTask(TaskType.DOCKER, "Nairobi","/usr/bin/uptime", image="ubuntu:latest")
  taskD = DagonTask(TaskType.DOCKER, "Mosco","cat workflow://Tokio/tokio.out", image="ubuntu:latest")

  workflow.add_task(taskA)
  workflow.add_task(taskB)
  workflow.add_task(taskC)
  workflow.add_task(taskD)

  taskB.add_dependency_to(taskA)
  taskC.add_dependency_to(taskA)
  taskD.add_dependency_to(taskB)
  taskD.add_dependency_to(taskC)

  jsonWorkflow=workflow.as_json()
  with open('taskflow-demo-docker.json', 'w') as outfile:
    stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
    outfile.write(stringWorkflow)
  
  workflow.run()
  


