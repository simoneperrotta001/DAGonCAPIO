Workflow launcher scripts:

- makeWpsNamelist.wrf5: Create the namelist for the WPS (geogrid, ungrib, metgrid).
- makeInputNamelist.wrf5: Create the namelist for the WRF (real and wrf).
- geogrid: Prepare the domains.
- ungrib: Extract the initial and boundary conditions from the initialization data.
- metgrid: Interpolate (regrid) the initial and boundary conditions on the model domains.
- real: Prepare the initial and boundary conditions for a real-world case.
- wrf: Run the model
- publishWrf: Processes the model outputs in a friendly-plottable netCDF file format.
