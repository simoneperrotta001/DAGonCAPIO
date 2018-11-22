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
    def find_ip_local(port):
        ip = dagon.read_config("dagon_ip")

        if ip is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        else:
            ip = ip['ip']

        return ip

    @staticmethod
    def check_url(url):
        url = "http://" + url

	response = requests.head(url)
	
	if response.status_code/100 > 2:
		return False;   
	return True;
        """try:
            conn = httplib.HTTPConnection(url)
            conn.request("HEAD", "/")
            r1 = conn.getresponse()
            print r1.status, r1.reason
            return True
        except gaierror, e:
	    print e
	    print url
            return False"""

        """try:
            response = API.requests_retry_session().get(
                url, timeout=2
            )
        except Exception as x:
            print('It failed :(', x.__class__.__name__)
            return False
        else:
            print('It eventually worked', response.status_code)
        finally:
            t1 = time.time()
            print('Took', t1 - t0, 'seconds')
        return True"""

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
