.. DagOn* documentation master file, created by
   sphinx-quickstart on Wed Nov 28 22:57:08 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Getting starting
==================================

Repository
********************
You can clone latest development version from our Git repository:
https://github.com/DagOnStar/dagonstar.git  

Prerequisites
********************

- Python 2.7

Installation using virtualenv
*****************************
.. sourcecode:: bash

    git clone https://github.com/DagOnStar/dagonstar.git  
    cd dagonstar  
    virtualenv venv  
    . venv/bin/activate  
    pip install -r requirements.txt  
    export PYTHONPATH=$PWD:$PYTHONPATH 

Using it
*****************************

This section describes how to use DagOn* to generate a simple data workflow.

1. Create a configuration file in the working directory, you could use the configuration sample file (``config.ini.sample``)

.. sourcecode:: bash

    cd dagonstar
    mkdir working_dir
    cp dagon.ini.sample working_dir/dagon.ini 
    cd working_dir

2. Create a new Python file and import the next modules.

.. sourcecode:: python

    from dagon import Workflow
    from dagon.task import DagonTask, Types

3. Create the workflow orchestation object with the ``workflow name``.

.. sourcecode:: python

    workflow = Workflow("DataFlow-Demo")

4. Define three simple batch tasks, passing as a parameter the ``command`` to execute. :code:`workflow:///A/output/f1.txt` means, that the file `outpu/f1.txt` have to be gotten from the ``scratch directory`` of the task `A`.

.. sourcecode:: python

    taskA = DagonTask(Types.BATCH, "A", "mkdir output;hostname > output/f1.txt")
    taskB = DagonTask(Types.BATCH, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt")
    taskC = DagonTask(Types.BATCH, "C", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt")
    taskD = DagonTask(Types.BATCH, "D", "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt")

5. Add the tasks to the workflow.

.. sourcecode:: python

    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

6. Generate the dependencies between tasks and run the workflow.

.. sourcecode:: python

    workflow.make_dependencies()
    workflow.run()

7. Putting all together.

.. sourcecode:: python

    from dagon import Workflow
    from dagon.task import DagonTask, Types

    workflow = Workflow("DataFlow-Demo")

    taskA = DagonTask(Types.BATCH, "A", "mkdir output;hostname > output/f1.txt")
    taskB = DagonTask(Types.BATCH, "B", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt")
    taskC = DagonTask(Types.BATCH, "C", "echo $RANDOM > f2.txt; cat workflow:///A/output/f1.txt >> f2.txt")
    taskD = DagonTask(Types.BATCH, "D", "cat workflow:///B/f2.txt >> f3.txt; cat workflow:///C/f2.txt >> f3.txt")

    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)
    workflow.add_task(taskD)

    workflow.make_dependencies()
    workflow.run()

You can find more examples with other kinds of tasks in `Examples <https://github.com/DagOnStar/dagonstar/tree/master/examples/>`_.