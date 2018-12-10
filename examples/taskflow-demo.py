from dagon import Workflow
from dagon.task import TaskType, DagonTask
import json

if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("Taskflow-Demo")

    taskA = DagonTask(TaskType.BATCH, "Tokio", "/bin/hostname >tokio.out")
    taskB = DagonTask(TaskType.BATCH, "Berlin", "/bin/date")
    taskC = DagonTask(TaskType.BATCH, "Nairobi", "/usr/bin/uptime")
    taskD = DagonTask(TaskType.BATCH, "Mosco", "cat workflow:///Tokio/tokio.out")

    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    taskB.add_dependency_to(taskA)
    taskC.add_dependency_to(taskA)
    taskD.add_dependency_to(taskB)
    taskD.add_dependency_to(taskC)

    jsonWorkflow = workflow.as_json()
    with open('taskflow-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    workflow.run()
