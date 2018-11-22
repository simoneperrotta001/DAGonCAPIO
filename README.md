# dagonstar
DAGon\* is a simple Python based workflow engine able to run job on everything from the local machine to distributed virtual HPC clusters hosted in private and public clouds.

####Installation 

```bash
git clone https://github.com/DagOnStar/dagonstar.git  
cd dagonstar  
virtualenv venv  
. venv/bin/activate  
pip install -r requirements.txt  
export PYTHONPATH=$PWD:$PYTHONPATH  
```

####Demo

Copy the configuration file in the examples directory.

```bash
cp dagon.ini.sample examples/dagon.ini 
cd examples
```

Edit the ini file matching your system configuration.

##Task oriented workflow.

The workflow is defined as tasks and their explicit dependencies.

```bash
python taskflow-demo.py
```

##Data oriented workflow.

The workflow is defined by data dependencies (task dependencies are automatically resolved)

```bash 
python dataflow-demo.py
```


### Batch Task Flow
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

### Batch Data Flow
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

