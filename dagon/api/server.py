
from flask import Flask, request
from flask import jsonify
from multiprocessing import Process
from threading import Thread
from flask_api import status
from urllib2 import urlopen
#from dagon import read_config
import dagon
from dagon.communication.connection import Connection
import requests

class WorkflowServer(Thread):
    
    def __init__(self, workflow, port):
        Thread.__init__(self)
        self.workflow = workflow
        self.app = Flask(__name__)
        self.port = port


    def run(self):
        #define all flask functions in a pythonic manner (functions inside functions)
        app = self.app
        @app.route('/api/<task>/info',methods = ['POST'])
        def info(task):
            if not request.is_json:
                return jsonify({"status": "error", "message": "Invalid JSON format"}), status.HTTP_400_BAD_REQUEST
            data = request.json
            task = self.workflow.find_task_by_name(task)
            if task is not None:
                task.set_info(data)
            self.workflow.name
            return jsonify(data)

        @app.route("/terminate", methods=['POST'])
        def terminate():
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()
            return 'Server shutting down...'

        self.app.run(port=self.port)

    def terminate(self):
        try:
            res = requests.post("http://localhost:%d/terminate"%self.port)
        except:
            pass
    
    def shutdown(self): 
        raise RuntimeError("Server going down")