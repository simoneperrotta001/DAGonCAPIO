import logging
import logging.config
from logging.config import fileConfig
from types import NoneType

from backports.configparser import NoSectionError
from requests.exceptions import ConnectionError
from graphviz import Digraph
from enum import Enum
from dagon.api import API
from dagon.api.server import WorkflowServer
from dagon.communication.connection import Connection

from dagon.batch import Batch
from dagon.batch import Slurm
from dagon.docker_task import DockerRemoteTask

from config import read_config


class Status(Enum):
    READY = "READY"
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class Workflow(object):
    SCHEMA = "workflow://"

    def __init__(self, name, cfg):
        fileConfig('dagon.ini')
        logging.getLogger("paramiko").setLevel(logging.WARNING)
        logging.getLogger("globus_sdk").setLevel(logging.WARNING)
        self.logger = logging.getLogger()

        self.name = name
        self.cfg = cfg
        self.dry = False
        self.tasks = []
        self.id = 0
        self.regist_on_api = False

        # to regist in the dagon service
        try:
            config = read_config('dagon_service')
            if config is not None:
                self.api = API(config['route'])
                self.regist_on_api = True
        except NoneType:
            self.logger.error("No dagon URL in config file")
        except NoSectionError:
            self.logger.error("No dagon URL in config file")
        except ConnectionError, e:
            self.logger.error(e)

        if self.regist_on_api:
            try:
                self.id = self.api.create_workflow(self)
                self.logger.debug("Workflow registration success id = %s" % self.id)
            except Exception, e:
                raise Exception(e)

        port = Connection.find_port()
        ip = Connection.find_ip_local(port)
	print ip
        self.workflow_server = WorkflowServer(self, ip, port)
        self.workflow_server.start()  # start workflow server
	self.url = "%s:%d" % (Connection.find_ip_public(), port)
	print self.url
        if not Connection.check_url(self.url):
            self.url = "%s:%d" % (ip, port)
        print self.url
        #elf.workflow_server.start()  # start workflow server

    def get_dry(self):
        return self.dry

    def set_dry(self, dry):
        self.dry = dry

    def get_url(self):
        return self.url

    def get_scratch_dir_base(self):
        return self.cfg['scratch_dir_base']

    def find_task_by_name(self, workflow_name, task_name):
        # Check if the workflow is the current one
        if workflow_name == self.name:

            # For each task in the tasks collection
            for task in self.tasks:

                # Check the task name
                if task_name == task.name:
                    # Return the result
                    return task

        return None

    def add_task(self, task):
        self.tasks.append(task)
        task.set_workflow(self)
        if self.regist_on_api:
            self.api.add_task(self.id, task)

    def make_dependencies(self):
        # Clean all dependencies
        for task in self.tasks:
            task.nexts = []
            task.prevs = []
            task.reference_count = 0

        # Automatically detect dependencies
        for task in self.tasks:
            # Invoke pre run
            task.pre_run()

    # Return a json representation of the workflow
    def asJson(self):
        jsonWorkflow = {"tasks": {}, "name": self.name, "id": self.id}
        for task in self.tasks:
            jsonWorkflow['tasks'][task.name] = task.as_json()
        return jsonWorkflow

    def run(self):
        self.logger.debug("Running workflow: %s", self.name)
        for task in self.tasks:
            task.start()

        for task in self.tasks:
            task.join()

        try:
            self.workflow_server.shutdown()
        except Exception, e:
            print e
            self.logger.debug("Server stopped %s", self.name)

    def draw(self):
        g = Digraph(self.name)
        g.node_attr.update(color='lightblue2', style='filled')
        for task in self.tasks:
            g.node(task.name, task.name)
            for child in task.nexts:
                g.edge(task.name, child.name)
        # g.view()


class DataMover(Enum):
    DONTMOVE = 0
    LINK = 1
    COPY = 2
    SCP = 3
    HTTP = 4
    HTTPS = 5
    FTP = 6
    SFTP = 7
    GRIDFTP = 8


class ProtocolStatus(Enum):
    ACTIVE = "active"
    DISACTIVE = "none"


class Stager(object):
    def __init__(self):
        pass

    def stage_in(self, dst_task, src_task, dst_path, local_path):
        data_mover = DataMover.DONTMOVE
        command = ""

        # ToDo: this have to be make automatic
        # get tasks info and select transference protocol
        dst_task_info = dst_task.get_info()
        src_task_info = src_task.get_info()

        if ((src_task.__class__ is Slurm or src_task.__class__ is Batch) and
            (dst_task.__class__ is Batch or dst_task.__class__ is Slurm)):
            data_mover = DataMover.LINK
        elif dst_task_info is not None and src_task_info is not None:  # check transference protocols and remote machine info if is availabel
            if dst_task_info['ip'] == src_task_info['ip']:
                data_mover = DataMover.LINK
            else:
                protocols = ["GRIDFTP", "SCP", "FTP"]
                for p in protocols:
                    if ProtocolStatus(src_task_info[p]) is ProtocolStatus.ACTIVE and \
                            ProtocolStatus(dst_task_info[p]) is ProtocolStatus.ACTIVE:
                        data_mover = DataMover[p]
                        break
        else: #best effort (SCP)
            data_mover = DataMover.SCP


        # Check if the symbolic link have to be used...
        if data_mover == DataMover.LINK:
            # Add the link command
            command = command + "# Add the link command\n"
            command = command + "ln -sf " + src_task.get_scratch_dir() + "/" + local_path + " " + dst_path + "/" + local_path + "\n\n"

        # Check if the copy have to be used...
        elif data_mover == DataMover.COPY:
            # Add the copy command
            command = command + "# Add the copy command\n"
            command = command + "cp -r " + src_task.get_scratch_dir() + "/" + local_path + " " + dst_path + "/" + local_path + "\n\n"

        # Check if the secure copy have to be used...
        elif data_mover == DataMover.SCP:
            # Add the copy command
            command = command + "# Add the secure copy command\n"
            command = command + "scp -r " + src_task.get_user() + "@" + src_task.get_ip() + ":" + src_task.get_scratch_dir() + "/" + local_path + " " + dst_path + "/" + local_path + "\n\n"

        return command


