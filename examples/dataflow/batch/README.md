# Batch Tasks with DagOnStar

### Demo 1 - Local Batch

The file ```dataflow-demo.py``` runs a workflow composed of four tasks.

#### Steps

Open the root directory of DagOnStar in a terminal, and run the following commands to prepare it.

```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD:$PYTHONPATH
```

Now navigate to the directory of the demo.

```bash
cd examples/dataflow/batch
```

Execute the file ```dataflow-demo.py``` as follows:

```bash
python dataflow-demo.py
```

When the execution of the workflow is completed, you must see an output on the terminal as follows:

```console
(venv) ciro@Ciro-De-Vita batch % python3 dataflow-demo.py                                       
2023-11-06 10:07:19,502 root         DEBUG    Running workflow: DataFlow-Demo
2023-11-06 10:07:19,502 root         DEBUG    A: Status.WAITING
2023-11-06 10:07:19,502 root         DEBUG    A: Status.RUNNING
2023-11-06 10:07:19,502 root         DEBUG    B: Status.WAITING
2023-11-06 10:07:19,502 root         DEBUG    A: Executing...
2023-11-06 10:07:19,502 root         DEBUG    C: Status.WAITING
2023-11-06 10:07:19,502 root         DEBUG    D: Status.WAITING
2023-11-06 10:07:19,502 root         DEBUG    A: Scratch directory: /tmp//1699261639503-A
2023-11-06 10:07:20,183 root         DEBUG    A Completed in 0.007085084915161133 seconds ---
2023-11-06 10:07:22,184 root         DEBUG    A: Status.FINISHED
2023-11-06 10:07:22,184 root         DEBUG    B: Status.RUNNING
2023-11-06 10:07:22,184 root         DEBUG    B: Executing...
2023-11-06 10:07:22,185 root         DEBUG    C: Status.RUNNING
2023-11-06 10:07:22,185 root         DEBUG    C: Executing...
2023-11-06 10:07:22,185 root         DEBUG    B: Scratch directory: /tmp//1699261642185-B
2023-11-06 10:07:22,185 root         DEBUG    C: Scratch directory: /tmp//1699261642185-C
2023-11-06 10:07:22,833 root         DEBUG    C Completed in 0.007802009582519531 seconds ---
2023-11-06 10:07:22,904 root         DEBUG    B Completed in 0.006855964660644531 seconds ---
2023-11-06 10:07:22,905 root         DEBUG    Removed /tmp//1699261639503-A
2023-11-06 10:07:24,838 root         DEBUG    C: Status.FINISHED
2023-11-06 10:07:24,906 root         DEBUG    B: Status.FINISHED
2023-11-06 10:07:24,907 root         DEBUG    D: Status.RUNNING
2023-11-06 10:07:24,907 root         DEBUG    D: Executing...
2023-11-06 10:07:24,907 root         DEBUG    D: Scratch directory: /tmp//1699261644907-D
2023-11-06 10:07:25,581 root         DEBUG    D Completed in 0.011127948760986328 seconds ---
2023-11-06 10:07:25,581 root         DEBUG    Removed /tmp//1699261642185-B
2023-11-06 10:07:25,582 root         DEBUG    Removed /tmp//1699261642185-C
2023-11-06 10:07:27,587 root         DEBUG    D: Status.FINISHED
2023-11-06 10:07:27,587 root         INFO     Workflow 'DataFlow-Demo' completed in 8.085587978363037 seconds ---
['25060\n', 'Ciro-De-Vita.local\n', '6405\n', 'Ciro-De-Vita.local\n']
```

### Demo 2 - Remote Batch

The file ```dataflow-demo-remote.py``` runs a workflow composed of four tasks on a remote machine. For each task specify the ip and username of remote machine.

#### Preconfigurations

1. **Generate an SSH Key Pair**
To begin, you need to generate an SSH key pair on your local machine. Open your terminal and run the following command:

```bash
ssh-keygen -t rsa -b 2048
```

This will generate a public key (usually named `id_rsa.pub`) and a private key (usually named `id_rsa`) in the `~/.ssh` directory.

2. **Copy the Public Key to the Server**
Now, you'll need to copy the public key to the server you want to access. You can use the `ssh-copy-id` command for this. Replace `username` and `server_ip` with your own information:

```bash
ssh-copy-id username@server_ip
```

This command will prompt you for the server password, and then it will copy your public key to the server's `~/.ssh/authorized_keys` file.

3. **Test the Key-Based Authentication**
Try to SSH into the server without a password to test if the key-based authentication is working:

```bash
ssh username@server_ip
```

If everything is set up correctly, you should be able to log in without entering a password.

Now you have successfully set up SSH access to your server without a password, using a key pair for authentication. Make sure to keep your private key secure on your local machine, and don't share it with anyone.

#### Steps

Open the root directory of DagOnStar in a terminal, and run the following commands to prepare it.

```bash
virtualenv venv
. venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD:$PYTHONPATH
```

Now navigate to the directory of the demo.

```bash
cd examples/dataflow/batch
```

Edit the file ```dataflow-demo-remote.py``` with the right SSH configurations for each task.

> [!WARNING]
> By now, the stager only supports the movement of data between remote machines or from a remote machine to a local machine. We are working on enable the stage in of data from a remote machine to a local machine.

Execute the file ```dataflow-demo-remote.py``` as follows:

```bash
python dataflow-demo-remote.py
```

When the execution of the workflow is completed, you must see an output on the terminal as follows:

```console
(venv) ciro@Ciro-De-Vita batch % python3 dataflow-demo-remote.py
2023-11-06 11:17:38,635 root         DEBUG    Running workflow: DataFlow-Demo-Remote
2023-11-06 11:17:38,636 root         DEBUG    A: Status.WAITING
2023-11-06 11:17:38,636 root         DEBUG    A: Status.RUNNING
2023-11-06 11:17:38,636 root         DEBUG    A: Executing...
2023-11-06 11:17:38,636 root         DEBUG    B: Status.WAITING
2023-11-06 11:17:38,636 root         DEBUG    C: Status.WAITING
2023-11-06 11:17:38,636 root         DEBUG    D: Status.WAITING
2023-11-06 11:17:38,879 root         DEBUG    A: Scratch directory: /tmp//1699265858636-A
2023-11-06 11:17:42,473 root         DEBUG    A Completed in 0.9358320236206055 seconds ---
2023-11-06 11:17:44,478 root         DEBUG    A: Status.FINISHED
2023-11-06 11:17:44,478 root         DEBUG    B: Status.RUNNING
2023-11-06 11:17:44,478 root         DEBUG    B: Executing...
2023-11-06 11:17:44,478 root         DEBUG    C: Status.RUNNING
2023-11-06 11:17:44,479 root         DEBUG    C: Executing...
2023-11-06 11:17:44,686 root         DEBUG    C: Scratch directory: /tmp//1699265864479-C
2023-11-06 11:17:44,687 root         DEBUG    B: Scratch directory: /tmp//1699265864479-B
2023-11-06 11:17:47,349 root         DEBUG    B Completed in 0.7923550605773926 seconds ---
2023-11-06 11:17:47,554 root         DEBUG    C Completed in 0.8206300735473633 seconds ---
2023-11-06 11:17:47,760 root         DEBUG    Removed /tmp//1699265858636-A
2023-11-06 11:17:49,351 root         DEBUG    B: Status.FINISHED
2023-11-06 11:17:49,765 root         DEBUG    C: Status.FINISHED
2023-11-06 11:17:49,766 root         DEBUG    D: Status.RUNNING
2023-11-06 11:17:49,766 root         DEBUG    D: Executing...
2023-11-06 11:17:49,951 root         DEBUG    D: Scratch directory: /tmp//1699265869766-D
2023-11-06 11:17:53,039 root         DEBUG    D Completed in 1.0582261085510254 seconds ---
2023-11-06 11:17:53,280 root         DEBUG    Removed /tmp//1699265864479-B
2023-11-06 11:17:53,514 root         DEBUG    Removed /tmp//1699265864479-C
2023-11-06 11:17:55,518 root         DEBUG    D: Status.FINISHED
2023-11-06 11:17:55,519 root         INFO     Workflow 'DataFlow-Demo-Remote' completed in 16.883061170578003 seconds ---
(venv) ciro@Ciro-De-Vita batch % 
```

You can see the results of the execution on the remote machine by accessing to the scratch directory of your tasks. For example, ```/tmp//1699265869766-D```.