#! /bin/bash
# This is the DagOn launcher script

code=0
# Change the current directory to the working directory
cd ./scratch/1697619842228-A
if [ $? -ne 0 ]; then code=1; fi 

# Start staging in



# Invoke the command
mkdir output;hostname > output/f1.txt |tee ./scratch/1697619842228-A/.dagon/stdout.txt
if [ $? -ne 0 ]; then code=1; fi

# Perform post process
exit $code