import sys
import datetime
import json

from dagon import Workflow, DataMover, StagerMover
from dagon.task import DagonTask, TaskType

# Check if this is the main
if __name__ == '__main__':
    # Set some default values

    # The model setup version
    wrf_model = "wrf5"
    roms_model = "rms3"
    wacomm_model = "wcm3"

    # Initialization date
    i_date = "20230724Z00"

    # Hours of simulation (168h -> 7days)
    hours = 24

    # By default perform a dry run (do not run the actual computation)
    dry = False

    # Check if the number of command line arguments is correct
    if len(sys.argv) == 6:

        # Set the inizialization date
        i_date = sys.argv[1]

        # Set the number of hours to simulate
        hours = int(sys.argv[2])

        # Set the models setup
        wrf_model = sys.argv[3]
        roms_model = sys.argv[4]
        wacomm_model = sys.argv[5]

        # Run the workflow with the actual computation
        dry = False
    else:
        # Perform a dry run with default parameters
        print("Defaults: " + str(wrf_model) + " " + str(roms_model) + " " + str(wacomm_model) + " " + str(i_date) + " " + str(hours) + " " + str(dry))

    # The application root
    envapp_root = "/beegfs/home/javier.garciablas/envapp/"

    # Where the restart are stored
    restart_dir_base_wrf = envapp_root + "/storage/data/opendap/" + wrf_model + "/restart/"
    restart_dir_base_wacomm = envapp_root + "/storage/data/opendap/" + wacomm_model + "/restart/"

    # Where the wrap scripts are
    command_dir_base_wrf = envapp_root + "/models/wrf/scripts/"
    command_dir_base_roms = envapp_root + "/models/roms/scripts"
    command_dir_base_wacomm = envapp_root + "/models/wacommplusplus/scripts/"

    # Application specific setup

    # The WRF domains
    domainItems = [ "d01", "d02", "d03"]

    # By default don't use restarts
    useRestart = "False"
    restartFile = ""

    # The ROMS domain
    # Grid: Hc=5 refined mask
    gridFilename = "/beegfs/home/javier.garciablas/envapp/models/roms/data/Campania_max200m_withC3andC4_angle0_hmin5.nc"
    domainId = "d03"

    # The WRF domain id for friction data
    wrfDomainId = "d02"

    # The Wacomm domain
    romsDomainId = "d03"

    # By default don't use restarts
    #useRestart = "False"
    #restartFile=""

    # Set a tentative restart file name
    
    #tentativeRestartFile = "{}/WACOMM_rst_{}.txt".format(restart_dir_base, i_date)

    # Check if the restart exists
    #if os.path.isfile(tentativeRestartFile):
    #  restartFile = tentativeRestartFile
    #  useRestart = "True"

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

    # Initial and final simulation dates in WRF format
    initial = start_date.strftime("%Y-%m-%d_%H:%M:%S")
    final = end_date.strftime("%Y-%m-%d_%H:%M:%S")

    # Create the restart date
    i_datetime = datetime.datetime.strptime(i_date, '%Y%m%dZ%H')
    #restart_datetime = i_datetime - datetime.timedelta(hours=1)
    restart_datetime = i_datetime
    restart_date = restart_datetime.strftime('%Y%m%dZ%H')

    # Create the orchestration workflow
    workflow = Workflow("ENVAPP")

    # Some beauty logging
    workflow.logger.info("initialization date: %s", i_date)

    # Set data mover policy (LINK/COPY)
    workflow.set_data_mover(DataMover.COPY)
    workflow.set_stager_mover(StagerMover.NORMAL)

    # The makeWpsNamelist task executed locally
    cmdNameList = "{}/makeWpsNamelist.{} {} {}".format(command_dir_base_wrf, wrf_model, initial, final)
    taskMakeWpsNameList = DagonTask(TaskType.BATCH, "makeWpsNameList", cmdNameList)

    # The geogrid task executed using Slurm
    cmdGeogrid = "{}/geogrid {} workflow:///makeWpsNameList/namelist.wps".format(command_dir_base_wrf, i_date)
    taskGeogrid = DagonTask(TaskType.SLURM, "geogrid", cmdGeogrid, partition="broadwell", ntasks=20, memory=40000)

    # Download the Copernicus data locally
    cmdDownloadCopernicus = "{}/downloadCopernicusData {} 7".format(command_dir_base_roms, i_date)
    taskDownloadCopernicusData = DagonTask(TaskType.BATCH, "downloadCopernicusData", cmdDownloadCopernicus)

    # The gfsdownloader task executed locally
    cmdGfsDownloader = "{}/gfsdownloader {} 50 {}".format(command_dir_base_wrf, i_date, hours)
    taskGfsDownloader = DagonTask(TaskType.BATCH, "gfsdownloader", cmdGfsDownloader)

    # The ungrib task executed using Slurm
    cmdUngrib = "{}/ungrib {} workflow:///gfsdownloader/data/{}/{}Z{} workflow:///makeWpsNameList/namelist.wps".format(command_dir_base_wrf, i_date, start_date.strftime("%Y%m%d"), start_date.strftime("%Y%m%d"), start_date.strftime("%H"))
    taskUngrib = DagonTask(TaskType.SLURM, "ungrib", cmdUngrib, partition="broadwell", ntasks=1, memory=40000)
    
    # the metgrid task executed using Slurm
    cmdMetgrid = "{}/metgrid {} workflow:///makeWpsNameList/namelist.wps workflow:///ungrib/FILE\* workflow:///geogrid/geo_em.\*".format(command_dir_base_wrf, i_date)
    taskMetgrid = DagonTask(TaskType.SLURM, "metgrid", cmdMetgrid, partition="broadwell", ntasks=40, memory=40000)

    # The makeInputNameList for the whole simulation executed locally
    cmdInputList = "{}/makeInputNamelist.{} {} {} 3 {} 1440".format(command_dir_base_wrf, wrf_model, i_date, f_date, useRestart)
    taskMakeInputNameList = DagonTask(TaskType.BATCH, "makeInputNameList", cmdInputList)

    # The real task executed using Slurm
    cmdReal = "{}/real {} workflow:///makeInputNameList/namelist.input workflow:///metgrid/met_em.\*".format(command_dir_base_wrf, i_date)
    taskReal = DagonTask(TaskType.SLURM, "real", cmdReal, partition="broadwell", ntasks=40, memory=40000)

    # The myocean2roms task executed using Slurm
    cmdMyoceans2roms = "{}/myocean2roms-2 {} {} {} workflow:///downloadCopernicusData/data/myoc_d00_\*".format(command_dir_base_roms, i_date, gridFilename, domainId)
    taskMyocean2roms = DagonTask(TaskType.SLURM, "myocean2roms", cmdMyoceans2roms, partition="broadwell", ntasks=1, memory=65000)
    
    # add tasks to the workflow
    workflow.add_task(taskMakeWpsNameList)
    workflow.add_task(taskGfsDownloader)
    workflow.add_task(taskDownloadCopernicusData)
    workflow.add_task(taskGeogrid)
    workflow.add_task(taskUngrib)
    workflow.add_task(taskMetgrid)
    workflow.add_task(taskMakeInputNameList)
    workflow.add_task(taskReal)
    workflow.add_task(taskMyocean2roms)

    cmdWrf2roms = "{}/wrf2roms.dist {} {}".format(command_dir_base_roms, gridFilename, domainId)

    days = int(hours/24)
    for day in range(0, days):
        i_date1 = start_date + datetime.timedelta(hours=24*day)
        f_date1 = start_date + datetime.timedelta(hours=24*(day+1))

        i_date1s = i_date1.strftime("%Y%m%dZ%H")
        f_date1s = f_date1.strftime("%Y%m%dZ%H")

        if day > 0:
            useRestart = "True"
            restartFile = "workflow:///wrf_" + str(day-1) + "/wrfrst_d\?\?_" + i_date1.strftime("%Y-%m-%d") + "_00:00:00"

        cmdInputList = "{}/makeInputNamelist.{} {} {} 3 {} 1440".format(command_dir_base_wrf, wrf_model, i_date1s, f_date1s, useRestart)
        taskMakeInputNameList = DagonTask(TaskType.BATCH, "makeInputNameList_" + str(day), cmdInputList)

        cmdWrf = "{}/wrf {} workflow:///makeInputNameList_{}/namelist.input workflow:///real/wrfbdy\* workflow:///real/wrfinput\* {}".format(command_dir_base_wrf, i_date, str(day), restartFile)
        taskWrf = DagonTask(TaskType.SLURM, "wrf_" + str(day), cmdWrf, partition="broadwell", ntasks=192, memory=128000)

        cmdPublishWrf = "{}/publishWrfOutput.test {} {} workflow:///wrf_{}/wrfout_*".format(command_dir_base_wrf, i_date, wrf_model, str(day))
        taskPublishWrf = DagonTask(TaskType.SLURM, "publishWrfOutput", cmdPublishWrf, partition="broadwell", ntasks=1, memory=8192)
        taskPublishWrf.set_mode("parallel")

        cmdPublishRoms = "{}/publishRomsOutput.test {}00 {}00 rms3 workflow:///roms/output/ocean_his_*".format(command_dir_base_roms, i_date, i_date)
        taskPublishRoms = DagonTask(TaskType.SLURM, "publishRomsOutput", cmdPublishRoms, partition="broadwell", ntasks=1, memory=8192)
        taskPublishRoms.set_mode("parallel")

        cmdPublishWacomm = "{}/publishWacommOutput.test {}00 {}00 wcm3 workflow:///wacommpp/output/wacomm_his_*".format(command_dir_base_wacomm, i_date, i_date)
        taskPublishWacomm = DagonTask(TaskType.SLURM, "publishWacommOutput", cmdPublishWacomm, partition="broadwell", ntasks=1, memory=8192)
        taskPublishWacomm.set_mode("parallel")

        cmdSaveRestart = "cp -r workflow:///wrf_{}/wrfrst_d??_{}_00:00:00 {}".format(str(day), f_date1.strftime("%Y-%m-%d"), restart_dir_base_wrf)
        taskSaveRestartWrf = DagonTask(TaskType.BATCH, "saveRestart_" + str(day), cmdSaveRestart)
        taskSaveRestartWrf.set_mode("parallel")

        cmdWrf2roms += " workflow:///wrf_{}/wrfout_{}\*".format(day, wrfDomainId)

        workflow.add_task(taskMakeInputNameList)
        workflow.add_task(taskWrf)
        workflow.add_task(taskSaveRestartWrf)
        workflow.add_task(taskPublishWrf)
        workflow.add_task(taskPublishRoms)
        workflow.add_task(taskPublishWacomm)

    # The roms task executed using Slurm
    taskWrf2roms = DagonTask(TaskType.SLURM, "wrf2roms", cmdWrf2roms, partition="broadwell", ntasks=1, memory=15000)

    # The roms task executed using Slurm
    cmdRoms = "{}/roms {} {} {} {} workflow:///myocean2roms/ini-{}.nc workflow:///myocean2roms/bry-{}.nc workflow:///wrf2roms/wind-{}.nc".format(command_dir_base_roms, i_date, str(hours), gridFilename, domainId, domainId, domainId, domainId)
    taskRoms = DagonTask(TaskType.SLURM, "roms", cmdRoms, partition="broadwell", ntasks=192, memory=128000)

    workflow.add_task(taskWrf2roms)
    workflow.add_task(taskRoms)

    # Load the restart
    #cmdLoadRestart = "cp {}/{}.nc __SELF__/{}.nc".format(restart_dir_base_wacomm, restart_date, restart_date)
    #taskLoadRestart = DagonTask(TaskType.BATCH, "wacommppLoadRestart", cmdLoadRestart)

    # The wacomm task executed using Slurm
    cmdWacomm = "{}/wacommpp {} {} {} workflow:///roms/output/".format(command_dir_base_wacomm, i_date, hours, romsDomainId)
    taskWacomm = DagonTask(TaskType.SLURM, "wacommpp", cmdWacomm, partition="broadwell", ntasks=48, memory=40000)

    # Save the restart
    #cmdSaveRestart = "cp -r workflow:///wacommpp/history/*.nc {}".format(restart_dir_base_wacomm)
    #taskSaveRestartWacomm = DagonTask(TaskType.BATCH, "wacommppSaveRestart", cmdSaveRestart)
    #taskSaveRestartWacomm.set_mode("parallel")

    #workflow.add_task(taskLoadRestart)
    workflow.add_task(taskWacomm)
    #workflow.add_task(taskSaveRestartWacomm)

    workflow.make_dependencies()

    jsonWorkflow = workflow.as_json()
    with open('/beegfs/home/javier.garciablas/envapp/workflow/envapp.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    if dry is False:
        workflow.run()
