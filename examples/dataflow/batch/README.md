# Remote tasks execution

## Preconfigurations

DagOnStar relies on SSH to coordinate tasks executed on remove machines. This SSH communication is based on public/private key authentication. Therefore, the first step is to configure your public key on the remote machine. Just follow the next steps:

1. First, you have to generate your public and private keys on the DagOnStar host.

    ```bash
    ssh-keygen -t rsa
    ```

    You've to complete the terminal questions. You must see an output as follows:

    ```console
    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/domizzi/.ssh/id_rsa): pruebasdagon
    Enter passphrase (empty for no passphrase): 
    Enter same passphrase again: 
    Your identification has been saved in pruebasdagon
    Your public key has been saved in pruebasdagon.pub
    The key fingerprint is:
    SHA256:xEKG8e/1aiKjEQaJsUmTOu7k4L1i0W0+M3CoGSGenf0 domizzi@domilaptop
    The key's randomart image is:
    +---[RSA 3072]----+
    |.o. .oo          |
    |.*...+ .         |
    |= o   o o        |
    |+. .   +         |
    |+.= B   S .      |
    |.B B * . . .     |
    |* * * . .   .    |
    | O . *oE. ..     |
    |. ..o.+o o.      |
    +----[SHA256]-----+
    ```

    > [!WARNING]
    > Please, create your keys without a passphrase. Now, DagOnStar doesn't support the management of ciphered keys.


2. Now, you have to copy your public key to the remote machine.

    ```bash
    cat /PATH/TO/PUBLIC/KEY.pub | ssh user@ip 'cat >> .ssh/authorized_keys'
    ```

3. Check your connection. You must be able to log in on the remote machine without a password.

    ```bash
    ssh -i /PATH/TO/PRIVATE/KEY user@ip
    ```

## Demo execution

1. Configure your SSH credentials con each DagOnStar remote task. You must to add the ```ip```, ```ssh_username```, and ```keypath``` parameters to the task definition, as follows:

    ```python
    remoteTask = DagonTask(TaskType.BATCH, "A", "mkdir output;hostname > output/f1.txt", ip="IP_To_Remote_Task", ssh_username="sshusername", keypath="/PATH/TO/PRIVATE/KEY")
    ```

2. Open the root directory of DagOnStar in a terminal, and run the following commands to prepare it.

    ```bash
    virtualenv venv
    . venv/bin/activate
    pip install -r requirements.txt
    export PYTHONPATH=$PWD:$PYTHONPATH
    ```

3. Now navigate to the directory of the demo and edit the file with the right SSH configurations for each task.

    ```bash
    cd examples/dataflow/batch
    ```

    > [!WARNING]
    > By now, the stager only supports the movement of data between remote machines or from a remote machine to a local machine. We are working on enable the stage in of data from a remote machine to a local machine.

4. Execute the file ```dataflow-demo-remote.py``` as follows:

    ```bash 
    python dataflow-demo-remote.py
    ```

    You'll see an output as follows:

    ```console
    2023-11-06 11:18:45,724 root         DEBUG    Running workflow: DataFlow-Demo-Remote
    2023-11-06 11:18:45,724 root         DEBUG    A: Status.WAITING
    2023-11-06 11:18:45,725 root         DEBUG    A: Status.RUNNING
    2023-11-06 11:18:45,725 root         DEBUG    A: Executing...
    2023-11-06 11:18:45,725 root         DEBUG    B: Status.WAITING
    2023-11-06 11:18:45,725 root         DEBUG    C: Status.WAITING
    2023-11-06 11:18:45,725 root         DEBUG    D: Status.WAITING
    2023-11-06 11:18:45,731 root         DEBUG    A: Scratch directory: /tmp//1699265925725-A
    2023-11-06 11:18:46,460 root         DEBUG    A Completed in 0.02167201042175293 seconds ---
    2023-11-06 11:18:48,463 root         DEBUG    A: Status.FINISHED
    2023-11-06 11:18:48,463 root         DEBUG    B: Status.RUNNING
    2023-11-06 11:18:48,463 root         DEBUG    B: Executing...
    2023-11-06 11:18:48,464 root         DEBUG    C: Status.RUNNING
    2023-11-06 11:18:48,464 root         DEBUG    C: Executing...
    2023-11-06 11:18:48,471 root         DEBUG    B: Scratch directory: /tmp//1699265928464-B
    2023-11-06 11:18:48,473 root         DEBUG    C: Scratch directory: /tmp//1699265928465-C
    2023-11-06 11:18:48,836 root         DEBUG    B Completed in 0.06079912185668945 seconds ---
    2023-11-06 11:18:48,840 root         DEBUG    C Completed in 0.06652951240539551 seconds ---
    2023-11-06 11:18:48,845 root         DEBUG    Removed /tmp//1699265925725-A
    2023-11-06 11:18:50,839 root         DEBUG    B: Status.FINISHED
    2023-11-06 11:18:50,847 root         DEBUG    C: Status.FINISHED
    2023-11-06 11:18:50,847 root         DEBUG    D: Status.RUNNING
    2023-11-06 11:18:50,847 root         DEBUG    D: Executing...
    2023-11-06 11:18:50,854 root         DEBUG    D: Scratch directory: /tmp//1699265930848-D
    2023-11-06 11:18:51,273 root         DEBUG    D Completed in 0.07032155990600586 seconds ---
    2023-11-06 11:18:51,279 root         DEBUG    Removed /tmp//1699265928464-B
    2023-11-06 11:18:51,286 root         DEBUG    Removed /tmp//1699265928465-C
    2023-11-06 11:18:53,287 root         DEBUG    D: Status.FINISHED
    2023-11-06 11:18:53,287 root         INFO     Workflow 'DataFlow-Demo-Remote' completed in 7.563155889511108 seconds ---
    ```

5. You can see the results of the execution on the remote machine by accessing to the scratch directory of your tasks. For example, ```/tmp//1699265930848-D```.