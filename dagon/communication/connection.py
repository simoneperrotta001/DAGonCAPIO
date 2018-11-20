import socket
from urllib2 import urlopen
import dagon
import requests
import socket


class Connection:

    #check if a port is open
    @staticmethod
    def isPortOpen(host, port, timeout=5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host,port))
        return result is 0

    @staticmethod
    def find_ip(port):
        ip = dagon.read_config("dagon_ip")

        if ip is None:
            ip = urlopen('http://ip.42.pl/raw').read()
            try:
                requests.get("http://%s:%d/check"%(ip,port), timeout=1)
            except:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
        else:
            ip = ip['ip']
        print ip
        return ip

    @staticmethod
    def find_port():
        PORTS = range(30000,30500)
        for i in PORTS:
            if not Connection.isPortOpen("localhost", i, timeout=1):
                return i