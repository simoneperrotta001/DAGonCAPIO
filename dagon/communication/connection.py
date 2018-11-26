import socket
from threading import Thread
from urllib2 import urlopen

import requests
import pickle


class Connection(Thread):

    def __init__(self, workflow, ip="127.0.0.1", port=9000):
        super(Connection, self).__init__()
        self.ip = ip
        self.port = port
        self.client = self.connect()
        self.info = None
        self.workflow = workflow

    def connect(self):
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((self.ip, self.port))
        return client

    def send_str(self, str):
        self.client.send(str)

    def run(self):
        while True:
            data = self.client.recv(1024)
            try:
                info = pickle.loads(data)
                if "task" in info:

                    task = self.workflow.find_task_by_name(self.workflow.name, info['task'])
                    task.set_info(info)
            except:
                pass

    def close_connection(self):
        self.client.close()

    # check if a port is open
    @staticmethod
    def is_port_open(host, port, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        return result is 0

    @staticmethod
    def find_ip_local(config_ip):
        ip = config_ip
        if ip is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]

        return ip

    @staticmethod
    def check_url(url):
        url = "http://" + url

        try:
            response = requests.head(url, timeout=3)
            if response.status_code / 100 > 2:
                return False
            return True
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError):
            return False

    @staticmethod
    def find_ip_public():
        ip = urlopen('http://ip.42.pl/raw').read()
        return ip

    @staticmethod
    def find_port():
        ports = range(30000, 30500)
        for i in ports:
            if not Connection.is_port_open("localhost", i, timeout=1):
                return i
