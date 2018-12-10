Workflow
==================================

The Workflow class of ``DagOn*`` allows you to manage the creation of data and task flows. In the data flows, it resolves the dependencies between tasks.

Terminology
************

Basic classes
--------------
- :class:`~dagon.Workflow` - represents a data or task workflow.
- :class:`~dagon.task.Task` - represents a task executed by a workflow.
- :class:`~dagon.task.TaskType` - represents where the task be executed. Supported techonologies are: ``batch``, ``cloud instances``, ``docker containers``, and ``slurm``.
- :class:`~dagon.Stager` - resolves the data dependencies between tasks.
- :class:`~dagon.DataMover` - represents a transference protocol/application. Supported techonologies are: ``ln (Unix)``, ``SCP``, and ``Globus GridFTP``.

Remote tasks
------------
- :class:`~dagon.RemoteTask` - represents a task executed on a remote machine.
- :class:`~dagon.communication.SSHManager` - manages the communication with a remote over ``SSH``.
- :class:`~dagon.communication.GlobusManager` - manages the exchange of data using ``Globus GridFTP``.
- :class:`~dagon.cloud.CloudManager` - manages communication with the cloud providers using `Apache Libcloud <https://libcloud.readthedocs.io>`_.

DagOn* Server
-------------
- :class:`~dagon.api.API` - performs the calls to the ``Dagon* service``.


Task types supported
*********************
There are four task types supported by ``DagOn*`` classified depending on the technology used to execute it. Also, ``Batch``, ``Docker``, and ``Slurm`` tasks can be executed on ``remote machines``. For more information see :doc:`task types page
</tasks_supported>`.
