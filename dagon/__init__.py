import logging
import logging.config
import os
from logging.config import fileConfig
from logging.config import dictConfig
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
from dagon.remote import RemoteTask


class Status(Enum):
    READY = "READY"
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class Workflow(object):
    SCHEMA = "workflow://"

    def __init__(self, name, config=None, config_file=None):

        if config is None and config_file is None:
            self.cfg = read_config('dagon.ini')
            fileConfig('dagon.ini')
        elif config is not None:
            self.cfg = config
        elif config_file is not None:
            self.cfg = read_config('dagon.ini')
            fileConfig(config_file)

        logging.getLogger("paramiko").setLevel(logging.WARNING)
        logging.getLogger("globus_sdk").setLevel(logging.WARNING)
        self.logger = logging.getLogger()

        self.name = name
        self.dry = False
        self.tasks = []
        self.id = 0
        self.regist_on_api = False

        # to regist in the dagon service
        try:
            self.api = API(self.cfg['dagon_service']['route'])
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
                """if "dagon_ip" in self.cfg and "ip" in self.cfg['dagon_ip']:
                    self.connection = Connection(self)
                    #self.connection.send_str(self.id)
                    #self.connection.start()"""
                self.logger.debug("Workflow registration success id = %s" % self.id)
            except Exception, e:
                raise Exception(e)

        """port = Connection.find_port()
        config_ip = None
        if "dagon_ip" in self.cfg and "ip" in self.cfg['dagon_ip']:
            config_ip = self.cfg['dagon_ip']['ip']
        ip = Connection.find_ip_local(config_ip)

        self.workflow_server = WorkflowServer(self, ip, port)
        self.workflow_server.start()  # start workflow server
        self.url = "%s:%d" % (Connection.find_ip_public(), port)
        if not Connection.check_url(self.url):
            self.url = "%s:%d" % (ip, port)
        self.logger.debug("Workflow server URL %s", self.url)"""

    def get_dry(self):
        return self.dry

    def set_dry(self, dry):
        self.dry = dry

    def get_url(self):
        return self.url

    def get_scratch_dir_base(self):
        return self.cfg['batch']['scratch_dir_base']

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
    def as_json(self):
        jsonWorkflow = {"tasks": {}, "name": self.name, "id": self.id}
        for task in self.tasks:
            jsonWorkflow['tasks'][task.name] = task.as_json()
        return jsonWorkflow

    def run(self):
        self.logger.debug("Running workflow: %s", self.name)
        for task in self.tasks:
            task.start()

        """for task in self.tasks:
            task.join()"""

        """try:
            self.workflow_server.shutdown()
        except Exception, e:
            print e
            self.logger.debug("Server stopped %s", self.name)"""

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
    INACTIVE = "inactive"


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


        # check transference protocols and remote machine info if is available
        if dst_task_info is not None and src_task_info is not None:
            if dst_task_info['ip'] == src_task_info['ip']:
                data_mover = DataMover.LINK
            else:
                protocols = ["GRIDFTP", "SCP", "FTP"]
                for p in protocols:
                    if ProtocolStatus(src_task_info[p]) is ProtocolStatus.ACTIVE:
                        data_mover = DataMover[p]
                        break
        else:  # best effort (SCP)
            data_mover = DataMover.LINK

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
            if isinstance(src_task, RemoteTask):  # if source is accesible from dest machine
                # copy my public key
                key = dst_task.get_public_key()
                src_task.add_public_key(key)

                command = command + "scp -o \"StrictHostKeyChecking no\" -i " + dst_task.working_dir + \
                          "/.dagon/ssh_key -r " + src_task.get_user() + "@" + src_task.get_ip() + ":" + \
                          src_task.get_scratch_dir() + "/" + local_path + " " + dst_path + "/" + local_path + "\n\n"
                command += "\nif [ $? -ne 0 ]; then code=1; fi"
                #command += "\n rm " + dst_task.working_dir + "/.dagon/ssh_key"
            else:  # if source is a local machine
                # copy my public key
                key = src_task.get_public_key()
                dst_task.add_public_key(key)

                command_mkdir = "mkdir -p " + dst_path + "/" + os.path.dirname(local_path) + "\n\n"
                res = dst_task.ssh_connection.execute_command(command_mkdir)

                if res['code']:
                    raise Exception("Couldn't create directory %s" % dst_path + "/" + os.path.dirname(local_path))

                command_local = "scp -o \"StrictHostKeyChecking no\" -i " + src_task.working_dir + \
                                "/.dagon/ssh_key -r " + src_task.get_scratch_dir() + "/" + local_path + " " + \
                                dst_task.get_user() + "@" + dst_task.get_ip() + ":" + dst_path + \
                                "/" + local_path + "\n\n"
                res = src_task.execute_command(command_local)

                if res['code']:
                    raise Exception("Couldn't copy data from %s to %s" % (src_task.get_ip(), dst_task.get_ip()))
        command += "\nif [ $? -ne 0 ]; then code=1; fi"

        return command
