from globus_sdk import TransferData, AccessTokenAuthorizer, TransferClient
import globus_sdk
import os 

class GlobusManager:
    """
    **Manages the data transference between two nodes using globus**

    :ivar _from: endpoint ID of the data source machine
    :vartype _from: str

    :ivar _to: endpoint ID of the data destiny machine
    :vartype _to: str

    """

    TRANSFER_TOKEN = "Ag82ObxMdj80Gxd2ex7oVJWJnPv4dWQ8ndkpblOz51n42G6g96i8Cjqx5zKDE4jM5E2OzrJgMYMlNYC7NmMbYfk6VP"

    def __init__(self, _from, _to, client_id, intermediate):

        """

        :param _from: endpoint ID of the data source machine
        :type _from: str

        :param _to:  endpoint ID of the data destiny machine
        :type _to: str
        """

        self._from = _from
        self._to = _to
        self.intermediate = intermediate

        # Initialize the Globus Native App Client to transfer data
        #print(client_id)

        client = globus_sdk.NativeAppAuthClient(client_id)
        client.oauth2_start_flow()
        authorize_url = client.oauth2_get_authorize_url()
        print(f"Please go to this URL and login:\n\n{authorize_url}\n")

        auth_code = input("Please enter the code you get after login here: ").strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)

        globus_auth_data = token_response.by_resource_server["auth.globus.org"]
        globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

        globus_auth_token = globus_auth_data["access_token"]
        transfer_token = globus_transfer_data["access_token"]

        #print("AUTH GLOBUS TOKEN    ", globus_auth_token)
        #print("TRANSFER TOKEN    ", transfer_token)
        
        authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
        self.transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
        # Crea un cliente de transferencia de Globus
        #transfer_token = globus_sdk.RefreshTokenAuthorizer(globus_auth_token, client)
        #self.transfer_client = globus_sdk.TransferClient(authorizer=transfer_token)

    def copy_directory(self, ori, destiny, intermediate):

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

        print("ori: ", self._from)
        print("destiny: ", self._to)

        task_data = globus_sdk.TransferData(
            source_endpoint=self._from, destination_endpoint=self._to
        )

        #transference_data = TransferData(tc, self._from,
        #                                 self._to,
        #                                 label="SDK example",
        #                                 sync_level="checksum")

        task_data.add_item(ori, destiny, recursive=True)

        transfer_result = self.transfer_client.submit_transfer(task_data)
        #transfer_result = tc.submit_transfer(transference_data)
        while not self.transfer_client.task_wait(transfer_result["task_id"], timeout=1):
            task = self.transfer_client.get_task(transfer_result["task_id"])

            if task['nice_status'] == "NOT_A_DIRECTORY":
                self.transfer_client.cancel_task(task["task_id"])
                return task['nice_status']
        return "OK"

    def copy_file(self, ori, destiny, intermediate):
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

        # transference_data = TransferData(tc, self._from,
        #                                  self._to,
        #                                  label="SDK example",
        #                                  sync_level="checksum")

        #copy the data from the source endpoint to the Globus intermediate endpoint

        task_data = globus_sdk.TransferData(
            source_endpoint=self._from, destination_endpoint=self.intermediate
        )
        
        task_data.add_item(ori, intermediate, recursive=False)
        transfer_result = self.transfer_client.submit_transfer(task_data)
        
        #print(f"Transferencia iniciada. ID de tarea: {transfer_result['task_id']}")

        while not self.transfer_client.task_wait(transfer_result["task_id"], timeout=1):
            continue # wait until transfer ends

        #copy the data from the source endpoint to the Globus intermediate endpoint

        task_data = globus_sdk.TransferData(
            source_endpoint=self.intermediate, destination_endpoint=self._to
        )

        task_data.add_item(intermediate, destiny, recursive=False)
        transfer_result = self.transfer_client.submit_transfer(task_data)
        
        #print(f"Transferencia iniciada. ID de tarea: {transfer_result['task_id']}")

        while not self.transfer_client.task_wait(transfer_result["task_id"], timeout=1):
            continue # wait until transfer ends


        return "OK"

    def copy_data(self, ori, destiny, intermediate):

        """
        copy data using globus

        :param ori: path where the data is in the source machine
        :type ori: str

        :param destiny: path where the data will be put on the destiny machine
        :type destiny: str

        :raises Exception: a problem occurred during the transference
        """

        #authorizer = AccessTokenAuthorizer(GlobusManager.TRANSFER_TOKEN)
        #tc = TransferClient(authorizer=authorizer)

        if os.path.isdir(ori):  # if the path is a directory
            res = self.copy_directory(ori, destiny, intermediate)
        else:
            res = self.copy_file(ori, destiny, intermediate)

        if res != "OK":
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
