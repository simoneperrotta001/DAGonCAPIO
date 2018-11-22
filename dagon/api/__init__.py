import requests
from requests.exceptions import ConnectionError

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# Perform the communication with the dagon service
class API:
    def __init__(self, url):
        self.base_url = url
        self.checkConnection()

    # check if the service URL is valid or a service is available
    def checkConnection(self):
        try:
            requests.get(self.base_url)
        except ConnectionError:
            raise ConnectionError("It is not possible connect to the URL %s" % self.base_url)

    # create workflow on dagon service
    def create_workflow(self, workflow):
        service = "/create"
        url = self.base_url + service
        data = workflow.as_json()
        res = requests.post(url, json=data)
        if res.status_code == 201:  # created
            json_reponse = res.json()
            return json_reponse['id']
        else:
            if res.status_code == 409:
                raise Exception("Workflow name already exists %d %s" % (res.status_code, res.reason))
            else:
                raise Exception("Workflow error registration %d %s" % (res.status_code, res.reason))

    # add task to workflow
    def add_task(self, workflow_id, task):
        service = "/add_task/%s" % workflow_id
        url = self.base_url + service
        data = task.as_json()
        res = requests.post(url, json=data)
        if res.status_code != 201 and res.status_code != 200:  # error
            raise Exception("Something went wrong %d %s" % (res.status_code, res.reason))

    # update a task status in the server
    def update_task_status(self, workflow_id, task, status):
        service = "/changestatus/%s/%s/%s" % (workflow_id, task, status)
        url = self.base_url + service
        res = requests.put(url)
        if res.status_code != 201 and res.status_code != 200:  # error
            raise Exception("Something went wrong %d %s" % (res.status_code, res.reason))

    # get a task from the server
    def get_task(self, workflow_id, task):
        service = "/update/%s/%s" % (workflow_id, task)
        url = self.base_url + service
        res = requests.get(url)
        if res.status_code != 201 and res.status_code != 200:  # error
            raise Exception("Something went wrong %d %s" % (res.status_code, res.reason))
        else:
            task = res.json()
            return task

    # update atribute of the task
    def update_task(self, workflow_id, task, attribute, value):
        service = "/update/%s/%s/%s?value=%s" % (workflow_id, task, attribute, value)
        url = self.base_url + service
        res = requests.put(url)
        if res.status_code != 201 and res.status_code != 200:  # error
            raise Exception("Something went wrong %d %s" % (res.status_code, res.reason))

    # add dependency on task
    def add_dependency(self, workflow_id, task, dependency):
        service = "/%s/%s/dependency/%s" % (workflow_id, task, dependency)
        url = self.base_url + service
        res = requests.put(url)
        if res.status_code != 201 and res.status_code != 200:  # error
            raise Exception("Something went wrong %d %s" % (res.status_code, res.reason))

    @staticmethod
    def requests_retry_session(
            retries=1,
            backoff_factor=0.3,
            status_forcelist=(500, 502, 504),
            session=None,
    ):
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session
