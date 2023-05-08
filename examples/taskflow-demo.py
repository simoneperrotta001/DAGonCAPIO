from dagon import Workflow
from dagon.task import TaskType, DagonTask
import json

if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("Taskflow-Demo")

    taskA = DagonTask(TaskType.BATCH, "A", "/bin/hostname > f_A.out")
    taskB = DagonTask(TaskType.BATCH, "B", "/bin/date")
    taskC = DagonTask(TaskType.BATCH, "C", "/usr/bin/uptime")
    taskD = DagonTask(TaskType.BATCH, "D", "cat workflow:///A/f_A.out > f_D.out")
    taskE = DagonTask(TaskType.BATCH, "E", "cat workflow:///D/f_D.out > f_E.out")

    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)
    workflow.add_task(taskE)

    taskB.add_dependency_to(taskA)
    taskC.add_dependency_to(taskA)
    taskD.add_dependency_to(taskB)
    taskD.add_dependency_to(taskC)
    taskE.add_dependency_to(taskD)

    jsonWorkflow = workflow.as_json()
    with open('taskflow-demo.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    workflow.run()
