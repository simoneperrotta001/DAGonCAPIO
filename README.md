# DagOnStar (aka DagOn\*)


DagOnStar (Direct acyclic graph On anything) is a lightweight Python library implementing
a workflow engine able to execute parallel jobs represented by direct acyclic graphs on
any combination of local machines, on-premise high-performance computing clusters,
containers, and cloud-based virtual infrastructures.

DagOnStar is used as the primary workflow engine to run real-world production-level
applications.

DagOnStar is in production at the [Center for Monitoring and Modeling Marine and Atmosphere](https:/meteo.uniparthenope.it)
applications hosted at the University of Naples "Parthenope".

### Motivation
Thanks to the advent of public, private, and hybrid clouds, the democratization of computational resources changed the rules in many science fields. For decades, one of the efforts of computer scientists and computer engineers was the development of tools able to simplify access to high-end computational resources by computational scientists. However, nowadays, any science field can be considered "computational" as the availability of powerful but easy-to-manage workflow engines is crucial.
### Cite DAGonStar

* Sánchez-Gallegos, Dante Domizzi, Diana Di Luccio, Sokol Kosta, J. L. Gonzalez-Compean, and Raffaele Montella.
  "An efficient pattern-based approach for workflow supporting large-scale science: The DagOnStar experience."
  Future Generation Computer Systems 122 (2021): 187-203.
  [link](https://www.sciencedirect.com/science/article/pii/S0167739X21000984)

* Sánchez-Gallegos, Dante D., Diana Di Luccio, José Luis Gonzalez-Compean, and Raffaele Montella.
  "Internet of things orchestration using dagon workflow engine."
  In 2019 IEEE 5th world forum on internet of things (WF-IoT), pp. 95-100. IEEE, 2019.
  [link](https://ieeexplore.ieee.org/abstract/document/8767199)

* Sánchez-Gallegos, Dante D., Diana Di Luccio, J. L. Gonzalez-Compean, and Raffaele Montella.
  "A microservice-based building block approach for scientific workflow engines: Processing large data volumes with dagonstar."
  In 2019 15th International Conference on Signal-Image Technology & Internet-Based Systems (SITIS), pp. 368-375. IEEE, 2019.
  [link](https://ieeexplore.ieee.org/abstract/document/9067951)

* Montella, Raffaele, Diana Di Luccio, and Sokol Kosta.
  "Dagon: Executing direct acyclic graphs as parallel jobs on anything."
  In 2018 IEEE/ACM Workflows in Support of Large-Scale Science (WORKS), pp. 64-73. IEEE, 2018.
  [link](https://ieeexplore.ieee.org/abstract/document/8638376)

#### Installation 

```bash
git clone https://github.com/DagOnStar/dagonstar.git  
cd dagonstar  
virtualenv venv  
. venv/bin/activate  
pip install -r requirements.txt  
export PYTHONPATH=$PWD:$PYTHONPATH  
```

#### Demo

Copy the configuration file in the examples directory.

```bash
cp dagon.ini.sample examples/dagon.ini 
cd examples
```

Edit the ini file matching your system configuration.

#### Task oriented workflow.

The workflow is defined as tasks and their explicit dependencies.

```bash
python taskflow-demo.py
```

#### Data oriented workflow.

The workflow is defined by data dependencies (task dependencies are automatically resolved)

```bash 
python dataflow-demo.py
```


#### Batch Task Flow
```python
    # Create the orchestration workflow
      workflow=Workflow("Taskflow-Demo",config)
    
      taskA=batch.Batch("Tokio","/bin/hostname >tokio.out")
      taskB=batch.Batch("Berlin","/bin/date")
      taskC=batch.Batch("Nairobi","/usr/bin/uptime")
      taskD=batch.Batch("Mosco","cat workflow://Tokio/tokio.out")
    
      workflow.add_task(taskA)
      workflow.add_task(taskB)
      workflow.add_task(taskC)
      workflow.add_task(taskD)
    
      taskB.add_dependency_to(taskA)
      taskC.add_dependency_to(taskA)
      taskD.add_dependency_to(taskB)
      taskD.add_dependency_to(taskC)
      
      workflow.run()
```

#### Batch Data Flow
```python
    from dagon import Workflow
    from dagon import batch
    import json
    import sys
    import datetime
    import os.path
    
    # Check if this is the main
    if __name__ == '__main__':
    
      config={
        "scratch_dir_base":"/tmp/test6",
        "remove_dir":False
      }
    
      # Create the orchestration workflow
      workflow=Workflow("DataFlow-Demo",config)
      
      # The task a
      taskA=batch.Batch("Tokio","mkdir output;ls > output/f1.txt")
      
      # The task b
      taskB=batch.Batch("Berlin","echo $RANDOM > f2.txt; cat workflow:///Tokio/output/f1.txt >> f2.txt")
      
      # The task c
      taskC=batch.Batch("Nairobi","echo $RANDOM > f2.txt; cat workflow:///Tokio/output/f1.txt >> f2.txt")
      
      # The task d
      taskD=batch.Batch("Mosco","cat workflow:///Berlin/f2.txt workflow:///Nairobi/f2.txt > f3.txt")
      
      # add tasks to the workflow
      workflow.add_task(taskA)
      workflow.add_task(taskB)
      workflow.add_task(taskC)
      workflow.add_task(taskD)
    
      workflow.make_dependencies()
    
      jsonWorkflow=workflow.asJson()
      with open('dataflow-demo.json', 'w') as outfile:
        stringWorkflow=json.dumps(jsonWorkflow,sort_keys=True, indent=2)
        outfile.write(stringWorkflow)
     
      # run the workflow
      workflow.run()
```
