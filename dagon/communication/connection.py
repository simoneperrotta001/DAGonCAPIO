from urllib2 import urlopen
import requests
import dagon
import time
import socket
from dagon.api import API
import httplib
from socket import gaierror

class Connection:

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
            print "entro"
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
