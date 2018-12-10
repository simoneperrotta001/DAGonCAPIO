from libcloud.compute.types import Provider

from dagon import Workflow, Batch
from dagon.docker_task import DockerTask
from dagon.remote import CloudTask
from dagon.cloud import KeyOptions
from dagon.task import DagonTask, TaskType

if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("CONAGUASlurm5")

    # ACQUISITION PHASE

    acq_cores = 16
    acq_state = "yuc camp qroo"
    acq_partitions = 10
    acq_volume = "data"
    acq_command = "java -jar acquisition.jar %d %d %s %s" % (acq_cores, acq_partitions, acq_state, acq_volume)

    task_acquisition = DagonTask(TaskType.BATCH, "ACQ" ,acq_command, ip="148.247.201.227", ssh_username="hreyes",
                                 working_dir="/home/hreyes/pruebas_dante/tasks/acquisition")
    # PARSING PHASE
    parsing_input = "workflow:///ACQ/%s/documentos/lote" % acq_volume
    parsing_command = "python /home/task/parser.py -i %s -o res"

    workflow.add_task(task_acquisition)

    for i in range(1, acq_partitions + 1):
        input_element = parsing_input + str(i)
        command = parsing_command % input_element

        task_parsing = DagonTask(TaskType.DOCKER, "P%d" % i, command,
                                 ip="ec2-34-208-132-217.us-west-2.compute.amazonaws.com",
                                 ssh_username="ubuntu", keypath="dagon_services.pem", image="ddomizzi/parser")
        workflow.add_task(task_parsing)

    # TRANSFORM THE DATA

    transform_command = "Rscript /home/batman/Transform.R "

    for i in range(1, acq_partitions + 1):
        transform_command += " workflow:///P%d/res" %i

    transform_command += " output"

    ssh_key_ec2 = {"option": KeyOptions.GET, "key_path": "dagon_services.pem"}
    #task_transform = DagonTask(TaskType.CLOUD, "T", transform_command, Provider.EC2, "ubuntu", ssh_key_ec2,
    #                           instance_id="i-0deec456d3418f9ac")
    task_transform = DagonTask(TaskType.SLURM, "I", transform_command, "testing", 5, ip="159.89.8.253", ssh_username="batman")
    workflow.add_task(task_transform)

    workflow.make_dependencies()

    # run the workflow
    workflow.run()
