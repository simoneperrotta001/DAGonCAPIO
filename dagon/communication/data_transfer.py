from globus_sdk import TransferData, AccessTokenAuthorizer, TransferClient


class GlobusManager:
    """
    **Manages the data transference between two nodes using globus**

    :ivar _from: endpoint ID of the data source machine
    :vartype _from: str

    :ivar _to: endpoint ID of the data destiny machine
    :vartype _to: str

    """

    TRANSFER_TOKEN = "Ag82ObxMdj80Gxd2ex7oVJWJnPv4dWQ8ndkpblOz51n42G6g96i8Cjqx5zKDE4jM5E2OzrJgMYMlNYC7NmMbYfk6VP"

    def __init__(self, _from, _to):

        """

        :param _from: endpoint ID of the data source machine
        :type _from: str

        :param _to:  endpoint ID of the data destiny machine
        :type _to: str
        """

        self._from = _from
        self._to = _to

    def copy_directory(self, ori, destiny, tc):

        """
        copy a directory using globus transfer

        :param ori: path where the data is in the source machine
        :type ori: str

        :param destiny: path where the data will be put on the destiny machine
        :type destiny: str

        :param tc: globus transfer client
        :type tc: :class:`globus_sdk.TransferClient`

        :return: status of the transference
        :rtype: str
        """

        transference_data = TransferData(tc, self._from,
                                         self._to,
                                         label="SDK example",
                                         sync_level="checksum")

        transference_data.add_item(ori, destiny, recursive=True)
        transfer_result = tc.submit_transfer(transference_data)
        while not tc.task_wait(transfer_result["task_id"], timeout=1):
            task = tc.get_task(transfer_result["task_id"])

            if task['nice_status'] == "NOT_A_DIRECTORY":
                tc.cancel_task(task["task_id"])
                return task['nice_status']
        return "OK"

    def copy_file(self, ori, destiny, tc):
        """
        copy a file using globus

        :param ori: path where the data is in the source machine
        :type ori: str

        :param destiny: path where the data will be put on the destiny machine
        :type destiny: str

        :param tc: globus transfer client
        :type tc: :class:`globus_sdk.TransferClient`

        :return: status of the transference
        :rtype: str
        """

        transference_data = TransferData(tc, self._from,
                                         self._to,
                                         label="SDK example",
                                         sync_level="checksum")

        transference_data.add_item(ori, destiny)
        transfer_result = tc.submit_transfer(transference_data)
        while not tc.task_wait(transfer_result["task_id"], timeout=1):
            # wait until transfer ends
            continue

        return "OK"

    def copy_data(self, ori, destiny):

        """
        copy data using globus

        :param ori: path where the data is in the source machine
        :type ori: str

        :param destiny: path where the data will be put on the destiny machine
        :type destiny: str

        :raises Exception: a problem occurred during the transference
        """

        authorizer = AccessTokenAuthorizer(GlobusManager.TRANSFER_TOKEN)
        tc = TransferClient(authorizer=authorizer)
        res = self.copy_directory(ori, destiny, tc)

        if res == "NOT_A_DIRECTORY":
            res = self.copy_file(ori, destiny, tc)

        if res is not "OK":
            raise Exception(res)


class SKYCDS:
    #TODO: Read this from configuration file
    CLIENT_TOKEN = "3c2d53762ec82cf4cb14f3c6d45601afaf4b2eb42c702fb9f9cc53fa874cf9a0"
    CATALOG_TOKEN = "b341db39dc182f276a4685ad4c0c8eb64bef1e7e1217655ff3da6eb28095670e"
    API_TOKEN = "16970b17feb38ad94a29443954487f8cde3221d2"
    IP_SKYCDS = ""

    def upload_data(self, task, path, mode="single", encryption=False):
        str_encryption = "true" if encryption else "false"
        command = "tar -czvf %s/data.tar %s --exclude=*.tar &&  docker exec -i client java -jar -Xmx3g -Xmx3g CP-ABE_ST_Up.jar %s %s %s %s bob 2 %s test %s" % \
                  (
                  task.get_scratch_dir(), path, SKYCDS.CLIENT_TOKEN, SKYCDS.API_TOKEN, SKYCDS.CATALOG_TOKEN, mode, path,
                  str_encryption)
        result = task.execute_command(command)
        return result

    def download_data(self, task, path):
        command = "mkdir -p %s && docker exec -i client java -jar -Xmx3g -Xmx3g CP-ABE_ST_Dow.jar %s %s %s %s 2 1 test %s" % \
                  (path, SKYCDS.CLIENT_TOKEN, SKYCDS.API_TOKEN, SKYCDS.CATALOG_TOKEN, SKYCDS.IP_SKYCDS, path)
        result = task.execute_command(command)
        return result
