# DAGonStar Examples


## Transversality

Dagon has the characteristic of transversality, allowing you to create dependencies in workflows towards already existing workflows and even in execution, or to build workflows of workflows for joint execution.

Requerimeints for Demos:
    
- [DagOnService](https://github.com/DagOnStar/DagOnService) deployed.
- URL to DagOnService Specified in dagon.ini file.
### Metaworkflows
---

Build of a metaworkflow composed of 2 different workflows: DataFlow-Demo-Server and DataFlow-traversal.
    DataFlow traversal depends on DataFlow-Demo-Server tasks A and C.

Execution commands:

    cd transversal
    python Transversal-demo.py


### Dependencies between workflows. Avoiding cycles.
---

Build of a metaworkflow composed of 3 different workflows with a cycle in their dependencies. 


Execution commands:

    cd transversal
    python Transversal-cycle.py

### Asynchronous dependency
---

 Example of an asynchronous traversal dependency. 

Execute a simple workflow with:

    cd transversal
    python WF1-transversal-async.py

Open another console and run the following workflows while the first one is still running:

    cd transversal
    python WF2-transversal-async.py

WF2-transversal-async.py contains a generic workflow in Dagon but with a dependency to _wf1-transversal-demo_ in WF1-traversal-async.py. 

Once tasks A and C of the _wf1-transversal-demo_ workflow are completed, the _wf2-transversal-demo_ workflow will continue its execution.
