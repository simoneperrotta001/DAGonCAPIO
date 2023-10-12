from dagon import Workflow, Slurm
from dagon import batch
import json
import time
import os
import sys
import datetime

# Check if this is the main
if __name__ == '__main__':
    # Set some default values

    # The model setup version
    wrf_model = "wrf5"

    # Initialization date
    i_date = "20180627Z00"

    # Hours of simulation (168h -> 7days)
    hours = 168

    # By default perform a dry run (do not run the actual computation)
    dry = True

    # Check if the number of command line arguments is correct
    if len(sys.argv) == 4:

        # Set the initialization date
        i_date = sys.argv[1]

        # Set the number of hours to simulate
        hours = int(sys.argv[2])

        # Set the model setup
        wrf_model = sys.argv[3]

        # Run the workflow with the actual computation
        dry = False
    else:
        # Perform a dry run with default parameters
        print("Defaults: " + str(wrf_model) + " " + str(i_date) + " " + str(hours) + " " + str(dry))

        # The application root
    prometeo_root = "/home/ccmmma/prometeo/"

    # Where the restart are stored
    restart_dir_base = "/mnt/webserv/ccmmma/prometeo/data/opendap/" + wrf_model + "/restart/"

    # Where the initialization data are stored
    data_dir_base = prometeo_root + "/data/ncep/"

    # Where the wrap scripts are
    command_dir_base = "/home/ccmmma/prometeo/models/scripts/"

    # Application specific setup

    # The WRF domains
    domainItems = ["d01", "d02", "d03"]

    # By default don't use restarts
    useRestart = "False"
    restartFile = ""

    # Create a datetime object for the initialization date
    i_year = i_date[:4]
    i_month = i_date[4:6]
    i_day = i_date[6:8]
    i_hour = i_date[-2:]

    start_date = datetime.datetime(int(i_year), int(i_month), int(i_day), int(i_hour))

    # Create a datetime object for the final date
    time_delta = datetime.timedelta(hours=hours)
    end_date = start_date + time_delta

    # The final date in NCEP format
    f_date = end_date.strftime("%Y%m%dZ%H")

    # Initialization data path
    data_dir = data_dir_base + start_date.strftime("%Y%m%d") + "/" + start_date.strftime(
        "%Y%m%d") + "Z" + start_date.strftime("%H")

    # Initial and final simulation dates in WRF format
    initial = start_date.strftime("%Y-%m-%d_%H:%M:%S")
    final = end_date.strftime("%Y-%m-%d_%H:%M:%S")

    # Create the orchestration workflow
    workflow = Workflow("CCMMMA")

    # Some beauty logging
    workflow.logger.info("initialization date: %s", i_date)
    workflow.logger.info("data dir: %s", data_dir)
    workflow.logger.info("initial: %s", initial)
    workflow.logger.info("final: %s", final)

    # The makeWpsNamelist task executed locally
    taskMakeWpsNameList = batch.Batch("makeWpsNameList",
                                      command_dir_base + "/makeWpsNamelist." + wrf_model + " " + initial + " " + final)

    # The geogrid task executed using Slurm
    taskGeogrid = Slurm("geogrid", command_dir_base + "/geogrid " + i_date +
                        " workflow:///makeWpsNameList/namelist.wps", "hicpu", 1)

    # The ungrib task executed using Slurm
    taskUngrib = Slurm("ungrib", command_dir_base + "/ungrib " + i_date + " " + data_dir +
                       " workflow:///makeWpsNameList/namelist.wps" , "hicpu", 1)

    # the metgrid task executed using Slurm
    taskMetgrid = Slurm("metgrid", command_dir_base + "/metgrid " + i_date +
                        " workflow:///makeWpsNameList/namelist.wps workflow:///ungrib/FILE\* workflow:///geogrid/geo_em.\*")

    taskMakeInputNameList = batch.Batch("makeInputNameList", command_dir_base + "/makeInputNamelist." + wrf_model + " "
                                        + i_date + " " + f_date + " 3 " + useRestart + " 1440")

    # The real task executed using Slurm
    taskReal = Slurm("real", command_dir_base + "/real " + i_date +
                     " workflow:///makeInputNameList/namelist.input workflow:///metgrid/met_em.\*", "hicpu", 1)

    # add tasks to the workflow
    workflow.add_task(taskMakeWpsNameList)
    workflow.add_task(taskGeogrid)
    workflow.add_task(taskUngrib)
    workflow.add_task(taskMetgrid)

    workflow.add_task(taskMakeInputNameList)
    workflow.add_task(taskReal)

    days = hours / 24

    for day in range(0, days):
        i_date1 = start_date + datetime.timedelta(hours=24 * day)
        f_date1 = start_date + datetime.timedelta(hours=24 * (day + 1))

        i_date1s = i_date1.strftime("%Y%m%dZ%H")
        f_date1s = f_date1.strftime("%Y%m%dZ%H")

        if day > 0:
            useRestart = "True"
            restartFile = "workflow:///wrf_" + str(day - 1) + "/wrfrst_d\?\?_" + \
                          i_date1.strftime("%Y-%m-%d") + "_00:00:00"
        taskMakeInputNameList = batch.Batch("makeInputNameList_" + str(day),
                                            command_dir_base + "/makeInputNamelist." + wrf_model + " " + i_date1s +
                                            " " + f_date1s + " 3 " + useRestart + " 1440")
        taskWrf = Slurm("wrf_" + str(day), command_dir_base + "/wrf " + i_date + " workflow://makeInputNameList_"
                        + str(day) + "/namelist.input workflow://real/wrfbdy\* workflow://real/wrfinput\* "
                        + restartFile)
        taskSaveRestart = batch.Batch("saveRestart_" + str(day),
                                      "cp -r workflow://wrf_" + str(day) + "/wrfrst_d??_" + f_date1.strftime(
                                          "%Y-%m-%d") + "_00:00:00 " + restart_dir_base)
        taskPublish = batch.Batch("publishWrfOutput_" + str(day),
                                  "sbatch --wait " + command_dir_base + "/publishWrfOutput " + i_date + " " + i_date1s + " " + wrf_model + " workflow://wrf_" + str(
                                      day) + "/wrfout_")

        workflow.add_task(taskMakeInputNameList)
        workflow.add_task(taskWrf)
        workflow.add_task(taskSaveRestart)
        workflow.add_task(taskPublish)

    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('wrf.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    workflow.run()