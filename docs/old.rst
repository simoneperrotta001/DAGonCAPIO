.. DagOn* documentation master file, created by
   sphinx-quickstart on Wed Nov 28 22:57:08 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to DagOn*'s documentation!
==================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Documentation for the Code
**************************
.. toctree::
   :maxdepth: 2
   :caption: Contents:

DagOn*
===================

.. inheritance-diagram:: dagon.Workflow dagon.Stager dagon.task.Task dagon.batch.Batch dagon.remote.RemoteTask dagon.remote.CloudTask dagon.docker_task.DockerTask dagon.docker_task.DockerRemoteTask dagon.batch.Slurm dagon.batch.RemoteSlurm
    :parts: 1

.. autoclass:: dagon.Workflow
   :members:

Stager
**************************
.. autoclass:: dagon.Stager
   :members:

.. autoclass:: dagon.ProtocolStatus
   :members:

.. autoclass:: dagon.DataMover
   :members:

Tasks
=====================

Supported Tasks
---------------

.. csv-table:: 
   :header: "Type", "Constant", "Module", "Documentation", "External docs"
   :widths: 15, 10, 15, 10, 10

   "Batch", "BATCH", "dagon.batch", "click", ""
   "CloudTask", "CLOUD", "dagon.remote", "click", ""
   "Docker", "DOCKER", "dagon.docker_task", "click", Docker_
   "Slurm", "SLURM", "dagon.batch", "click", Slurm_

.. _Docker: https://www.docker.com/
.. _Slurm: https://slurm.schedmd.com/

.. automodule:: dagon.task
   :members:

.. autoclass:: dagon.remote.RemoteTask
   :members:

Batch tasks
**************************
.. autoclass:: dagon.batch.Batch
   :members:

.. autoclass:: dagon.batch.RemoteBatch
   :members:

Slurm tasks
****************************
.. autoclass:: dagon.batch.Slurm
   :members:

.. autoclass:: dagon.batch.RemoteSlurm
   :members:

Cloud task
****************************
.. autoclass:: dagon.remote.CloudTask
   :members:

Docker tasks
**************************
.. autoclass:: dagon.docker_task.DockerTask
   :members:

.. autoclass:: dagon.docker_task.DockerRemoteTask
   :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
