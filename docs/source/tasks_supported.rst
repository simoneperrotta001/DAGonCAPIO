.. DagOn* documentation master file, created by
   sphinx-quickstart on Wed Nov 28 22:57:08 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Tasks supported
==================================

.. csv-table:: 
   :header: "Type", "Constant", "Module", "Documentation", "External docs"
   :widths: 15, 10, 15, 10, 10

   "Batch", "BATCH", :class:`dagon.batch`, :class:`~dagon.batch.Batch`/:class:`~dagon.batch.RemoteBatch`, ""
   "CloudTask", "CLOUD", :class:`dagon.remote`, :class:`~dagon.remote.CloudTask`, `Apache Libcloud <https://libcloud.readthedocs.io>`_
   "Docker", "DOCKER", :class:`dagon.docker_task`, :class:`~dagon.docker_task.DockerTask`/:class:`~dagon.docker_task.DockerRemoteTask`, Docker_
   "Slurm", "SLURM", :class:`dagon.batch`, :class:`~dagon.batch.Slurm`/:class:`~dagon.batch.RemoteSlurm`, Slurm_

.. _Docker: https://www.docker.com/
.. _Slurm: https://slurm.schedmd.com/


.. inheritance-diagram:: dagon.task.Task dagon.batch.Batch dagon.remote.RemoteTask dagon.remote.CloudTask dagon.docker_task.DockerTask dagon.docker_task.DockerRemoteTask dagon.batch.Slurm dagon.batch.RemoteSlurm
    :parts: 1


Batch task
***************
Task executed on the default shell (commonly `bash`) of the operating system of the machine when is called. It is composed of one or more instructions sequentially executed. This type of class can be executed both, locally or in a remote machine. In the second case an valid SSH credentials must be provided.

Local batch task
-------------------
Regular Batch task that runs on the same machine where the user called ``DagOn*``.

.. sourcecode:: python

    taskA = DagonTask(Types.BATCH, "A", "mkdir output;hostname > output/f1.txt")


Remote batch task
------------------
Batch task runs over a remote machine. Regular Batch tasks takes this behavior where is passed as a argument the ``IP Address``, ``username`` and if it is necessary the path to the ``private key``.

.. sourcecode:: python

    taskA = DagonTask(Types.BATCH, "A", "mkdir output;hostname > output/f1.txt", ip="111.111.111.111", ssh_username="user", keypath="/path/to/key")


Cloud task
***************

Docker task
***************

Slurm task
***************