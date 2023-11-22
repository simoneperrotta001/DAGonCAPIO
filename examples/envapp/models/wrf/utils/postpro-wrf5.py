from netCDF4 import Dataset, date2num
from wrf import getvar, interplevel
import sys
from os.path import basename
from datetime import timedelta, date, datetime
import numpy as np
from scipy.interpolate import griddata
import math

if len(sys.argv)!=5:
  print ("Usage: python "+str(sys.argv[0])+" initialization_date source_file history_dir destination_file")
  sys.exit(-1)              

iDate=sys.argv[1]
src=sys.argv[2]
history_dir=sys.argv[3]
dst=sys.argv[4]

print ("iDate:"+iDate+" src: "+src+" history: "+history_dir+" dst: "+dst)

parts=basename(src).split("_")
prod=parts[0]
domain=parts[1]

# Open the NetCDF file
ncsrcfile = Dataset(src)

datetimeStr = ''.join(ncsrcfile.variables["Times"][:][0]).split("_")
dateStr=datetimeStr[0].split("-")
timeStr=datetimeStr[1].split(":")

datetime_current=datetime(int(dateStr[0]),int(dateStr[1]),int(dateStr[2]),int(timeStr[0]),int(timeStr[1]),int(timeStr[2]))
datetime_1h_ago=datetime_current - timedelta(hours=1)
datetime_00=datetime(int(dateStr[0]),int(dateStr[1]),int(dateStr[2]),0,0,0)

dates=[ datetime_current ]
print ("Dates -- current: "+str(datetime_current)+" 1h ago: "+str(datetime_1h_ago)+" today: "+str(datetime_00))

def get_date_time(date):
    dateTime=format(date.year,'04')+format(date.month,'02')+format(date.day,'02')+"Z"+format(date.hour,'02')+format(date.minute,'02')
    return dateTime

def get_date_time_path(date):
    dateTimePath=format(date.year,'04')+"/"+format(date.month,'02')+"/"+format(date.day,'02')
    return dateTimePath

src_1hago=history_dir+"/"+get_date_time_path(datetime_1h_ago)+"/"+prod+"_"+domain+"_"+get_date_time(datetime_1h_ago)+".nc"
src_00=history_dir+"/"+get_date_time_path(datetime_00)+"/"+prod+"_"+domain+"_"+get_date_time(datetime_00)+".nc"

print ("Prev       : "+str(src_1hago))
print ("Current day: "+str(src_00))

Xlat = np.array(getvar(ncsrcfile, "XLAT",meta=False))
Xlon = np.array(getvar(ncsrcfile, "XLONG",meta=False))

lon = np.average(Xlon)
lat = np.average(Xlat)

#Earth's radius, sphere
R=6378137

#offsets in meters
dn = ncsrcfile.DY
de = ncsrcfile.DX

#Coordinate offsets in degrees
dLat = 0.5*(dn/R)*180/math.pi
dLon = 0.5*(de/(R*math.cos(math.pi*lat/180)))*180/math.pi

def getBoundaries(Xlon, Xlat):
    row_lat = len(Xlat) - 1
    col_lat = len(Xlat[0]) - 1

    row_long = len(Xlon) - 1
    col_long = len(Xlon[0]) - 1

    A = [Xlat[0][0], Xlon[0][0]]
    B = [Xlat[0][col_lat], Xlon[0][col_long]]
    C = [Xlat[row_lat][col_lat], Xlon[row_long][col_long]]
    D = [Xlat[row_lat][0], Xlon[row_long][0]]

    min_lat = Xlat[0][0]
    minI = 0

    ''' from A to B '''
    for i in xrange(col_lat,-1,-1):
        np1 = [Xlat[0][i], Xlon[0][i]]
        if np1[0] > min_lat:
            minI = i
            min_lat = np1[0]

    max_lat = Xlat[row_lat][col_lat]
    maxI = col_lat

    ''' from C to D '''
    for i in xrange(col_lat,-1,-1):
        np1 = [Xlat[row_lat][i], Xlon[row_long][i]]
        if np1[0] < max_lat:
            maxI = i
            max_lat = np1[0]

    min_long = Xlon[0][0]
    minJ = 0

    ''' from A to D '''
    for i in xrange(row_lat,-1,-1):
        np1 = [Xlat[i][0], Xlon[i][0]]
        if np1[1] > min_long:
            minJ = i
            min_long = np1[1]

    max_long = Xlon[0][col_long]
    maxJ = row_lat

    ''' from B to C '''
    for i in xrange(row_lat,-1,-1):
        np1 = [Xlat[i][col_lat], Xlon[i][col_lat]]
        if np1[1] < max_long:
            maxJ = i
            max_long = np1[1]


    minLat=np.asscalar(min_lat)
    maxLat=np.asscalar(max_lat)
    minLon=np.asscalar(min_long)
    maxLon=np.asscalar(max_long)

    return minLon,minLat,maxLon,maxLat

def interp( srcLons,srcLats,invar2d,dstLons,dstLats ):
    py = srcLats.flatten()
    px = srcLons.flatten()
    z = np.array(invar2d).flatten()
    z[z == 1.e+37]='nan'
    X, Y = np.meshgrid(dstLons, dstLats)
    outvar2d = griddata((px,py),z, (X, Y),method='linear',fill_value=1.e+37)
    return outvar2d

# Calculate the actual boundaries
minLon,minLat,maxLon,maxLat=getBoundaries(Xlon,Xlat)

# Create the latitude array
lats =  np.arange(minLat,maxLat,dLat)

# create the longitude array
lons =  np.arange(minLon,maxLon,dLon)

# the number of colums
ncols = len(lons)

# the number of rows
nrows = len(lats)

print ("minLon,minLat:"+str(minLon)+","+str(minLat))
print ("maxLon,maxLat:"+str(maxLon)+","+str(maxLat))
print ("dLon:"+str(dLon))
print ("dLat:"+str(dLat))
print ("ncols:"+str(ncols))
print ("nrows:"+str(nrows))

print ("Interpolating...")

# Extract the pressure, geopotential height, temperature
p = getvar(ncsrcfile, "pressure")
z = getvar(ncsrcfile, "z", units="dm")
tc= getvar(ncsrcfile, "temp",units="degC")
td= getvar(ncsrcfile, "td", units="degC")
ter = getvar(ncsrcfile,"ter",units="m")

theta_e= getvar(ncsrcfile, "theta_e", units="degC")
theta_w= getvar(ncsrcfile, "twb", units="degC")
rh = getvar(ncsrcfile, "rh")
uvmet = getvar(ncsrcfile, "uvmet")

tc1000  = interplevel(tc, p, 1000)
rh1000  = interplevel(rh, p, 1000)
u1000  = interplevel(uvmet[0], p, 1000)
v1000  = interplevel(uvmet[1], p, 1000)

tc975  = interplevel(tc, p, 975)
rh975  = interplevel(rh, p, 975)
u975  = interplevel(uvmet[0], p, 975)
v975  = interplevel(uvmet[1], p, 975)

tc950 = interplevel(tc, p, 950)
rh950  = interplevel(rh, p, 950)
u950  = interplevel(uvmet[0], p, 950)
v950  = interplevel(uvmet[1], p, 950)

tc925  = interplevel(tc, p, 925)
rh925  = interplevel(rh, p, 925)
u925  = interplevel(uvmet[0], p, 925)
v925  = interplevel(uvmet[1], p, 925)

gph850 = interplevel(z, p, 850)
theta_e850 = interplevel(theta_e, p, 850)
theta_w850 = interplevel(theta_w, p, 850)
td850 = interplevel(td, p, 850)
tc850 = interplevel(tc, p, 850)
rh850  = interplevel(rh, p, 850)
u850  = interplevel(uvmet[0], p, 850)
v850  = interplevel(uvmet[1], p, 850)

td700 = interplevel(td, p, 700)
tc700 = interplevel(tc, p, 700)
rh700  = interplevel(rh, p, 700)
u700  = interplevel(uvmet[0], p, 700)
v700  = interplevel(uvmet[1], p, 700)

theta_e500 = interplevel(theta_e, p, 500)
gph500 = interplevel(z, p, 500)
tc500 = interplevel(tc, p, 500)
rh500  = interplevel(rh, p, 500)
u500  = interplevel(uvmet[0], p, 500)
v500  = interplevel(uvmet[1], p, 500)

tc300 = interplevel(tc, p, 300)
rh300  = interplevel(rh, p, 300)
u300  = interplevel(uvmet[0], p, 300)
v300  = interplevel(uvmet[1], p, 300)

tt = tc850 + td850 - 2*tc500  
ki = (tc850-tc500)+td850-(tc700-td700)
delta_theta = theta_e500-theta_e850

cape_2d = getvar(ncsrcfile,"cape_2d",meta=False)
updraft_helicity = getvar(ncsrcfile,"updraft_helicity",meta=False)
helicity = getvar(ncsrcfile,"helicity",meta=False)

# Get the sea level pressure
slp = getvar(ncsrcfile, "slp",meta=False)
rh2 = getvar(ncsrcfile, "rh2",meta=False)
pw = getvar(ncsrcfile, "pw",meta=False)
cloudfrac=getvar(ncsrcfile, "cloudfrac",meta=False)

# Cloud fraction as the maximum of the low and mid layers
clf=np.maximum(cloudfrac[0],cloudfrac[1])

# Read the temperature at 2m in celsusio
t2c=ncsrcfile.variables["T2"][:]-273.15

# Read the snow rate
sr=ncsrcfile.variables["SR"][:]

# Interpolate the snow rate
sri=interp(Xlon,Xlat,sr[0],lons,lats)

# Get the wind at 10m u and v components (meteo oriented)
uvmet10=getvar(ncsrcfile, "uvmet10",meta=False)

# Get the wind speed and wind dir at 10m (meteo oriented)
uvmet10_wspd_wdir=getvar(ncsrcfile, "uvmet10_wspd_wdir",meta=False)

# Interpolate wind speed and wind dir at 10m
wspd10i = interp(Xlon,Xlat,uvmet10_wspd_wdir[0],lons,lats)
wdir10i = interp(Xlon,Xlat,uvmet10_wspd_wdir[1],lons,lats)


# Read the simulation cumulated rain from the current file
rain=ncsrcfile.variables["RAINC"][:]+ncsrcfile.variables["RAINNC"][:]+ncsrcfile.variables["RAINSH"][:]

# Interpolate the cumulated rain
raini=interp(Xlon,Xlat,rain[0],lons,lats)

# Set to none the h00 (daily) interpolated values
raini_00=None
wspd10i_00=None
wdir10i_00=None

try:
    # Try to open the previous hour dataset
    ncsrc_00 = Dataset(src_00)

    print ("Calculating daily deltas...")

    # Read the simulation cumulated rain from the current file
    rain_00=ncsrc_00.variables["RAINC"][:]+ncsrc_00.variables["RAINNC"][:]+ncsrc_00.variables["RAINSH"][:]

    # Interpolate the cumulated rain
    raini_00=interp(Xlon,Xlat,rain_00[0],lons,lats)

    # Close the dataset
    ncsrc_00.close()

    print ("...done with daily processing.")

except:
    # If not previous hour, just calculate from 0
    print ("WARNING *** Troubles with the daily dataset: "+src_00)

    # Hourly rain
    raini_00=raini

# Calculate the hourly cumulated rain
print ("Daily Rain...")
draini=raini-raini_00

# Set to none the 1 hour ago interpolated values
raini_1hago=None
wspd10i_1hago=None
wdir10i_1hago=None

try:
    # Try to open the previous hour dataset
    ncsrc_1hago = Dataset(src_1hago)

    print ("Calculating 1 hour ago deltas...")

    # Read the simulation cumulated rain from the current file
    rain_1hago=ncsrc_1hago.variables["RAINC"][:]+ncsrc_1hago.variables["RAINNC"][:]+ncsrc_1hago.variables["RAINSH"][:]

    # Interpolate the cumulated rain
    raini_1hago=interp(Xlon,Xlat,rain_1hago[0],lons,lats)

    # Get the wind speed and wind dir at 10m (meteo oriented)
    uvmet10_wspd_wdir_1hago=getvar(ncsrc_1hago, "uvmet10_wspd_wdir",meta=False)

    # Interpolate wind speed and wind dir at 10m
    wspd10i_1hago = interp(Xlon,Xlat,uvmet10_wspd_wdir_1hago[0],lons,lats)
    wdir10i_1hago = interp(Xlon,Xlat,uvmet10_wspd_wdir_1hago[1],lons,lats)

    # Close the dataset
    ncsrc_1hago.close()

    print ("...done with 1 hour ago processing.")

except:
    # If not previous hour, just calculate from 0
    print ("WARNING *** Troubles with the previous dataset: "+src_1hago)

    # Hourly rain
    raini_1hago=raini

    # Wind shift
    wspd10i_1hago=wspd10i
    wdir10i_1hago=wdir10i

# Calculate the hourly cumulated rain
print ("Rain...")
hraini=raini-raini_1hago

# Calculate the wind shift
print ("Wind shift...")
dwspd10i=wspd10i-wspd10i_1hago
dwdir10i=wdir10i-wdir10i_1hago

# Calc snow water equivalent
hswei=np.array(hraini*(sri-0.75)*5)
hswei[hswei < 0]=0

ncdstfile = Dataset(dst, "w", format="NETCDF4")

# Write iDate attribute
ncdstfile.IDATE=iDate

timeDim=ncdstfile.createDimension("time", size=1)
timeDim=ncdstfile.createDimension("latitude", size=nrows)
timeDim=ncdstfile.createDimension("longitude", size=ncols)

timeVar = ncdstfile.createVariable("time", "i4",("time"))
timeVar.description="Time";
timeVar.long_name="time";
timeVar.units="hours since 1900-01-01 00:00:0.0";
timeVar.standard_name = "time" ;
timeVar.calendar = "standard" ;
timeVar.axis = "T" ;


lonVar = ncdstfile.createVariable("longitude", "f4",("longitude"))
lonVar.description="Longitude";
lonVar.long_name="longitude"
lonVar.units="degrees_east";
lonVar.standard_name = "longitude" ;
lonVar.axis = "X" ;

latVar = ncdstfile.createVariable("latitude", "f4",("latitude"))
latVar.description="Latitude";
latVar.long_name="latitude"
latVar.units="degrees_north";
latVar.standard_name = "latitude" ;
latVar.long_name = "latitude" ;
latVar.units= "degrees_north" ;
latVar.axis = "Y" ;


hsweVar = ncdstfile.createVariable("HOURLY_SWE", "f4",("time","latitude","longitude"),fill_value=1.e+37)
hsweVar.description="Snow water equivalent";
hsweVar.units="kg m-2";

hrainVar = ncdstfile.createVariable("DELTA_RAIN", "f4",("time","latitude","longitude"),fill_value=1.e+37)
hrainVar.description="Hourly cumulated rain";
hrainVar.units="mm";

drainVar = ncdstfile.createVariable("DAILY_RAIN", "f4",("time","latitude","longitude"),fill_value=1.e+37)
drainVar.description="Daily cumulated rain";
drainVar.units="mm";

t2cVar = ncdstfile.createVariable("T2C", "f4",("time","latitude","longitude"),fill_value=1.e+37)
t2cVar.description="Temperature at 2m in Celsius";
t2cVar.units="C";

rh2Var = ncdstfile.createVariable("RH2", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh2Var.description="Relative humidity at 2 meters";
rh2Var.units="%";

pwVar = ncdstfile.createVariable("PW", "f4",("time","latitude","longitude"),fill_value=1.e+37)
pwVar.description="Precipitable Water";
pwVar.units="kg m-2"

uhVar = ncdstfile.createVariable("UH", "f4",("time","latitude","longitude"),fill_value=1.e+37)
uhVar.description="Updraft Helicity";
uhVar.units="m2 s-2";

srhVar = ncdstfile.createVariable("SRH", "f4",("time","latitude","longitude"),fill_value=1.e+37)
srhVar.description="Storm Relative Helicity";
srhVar.units="m2 s-2"

mcapeVar = ncdstfile.createVariable("MCAPE", "f4",("time","latitude","longitude"),fill_value=1.e+37)
mcapeVar.description="Most unstable convective available potential energy";
mcapeVar.units="J kg-1";

mcinVar = ncdstfile.createVariable("MCIN", "f4",("time","latitude","longitude"),fill_value=1.e+37)
mcinVar.description="Maximum convective inibition";
mcinVar.units="J kg-1";

u1000Var = ncdstfile.createVariable("U1000", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u1000Var.description="grid rel. x-wind component at 1000 HPa";
u1000Var.standard_name="u-component"
u1000Var.units="m s-1"

v1000Var = ncdstfile.createVariable("V1000", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v1000Var.description="grid rel. y-wind component at 1000 HPa";
v1000Var.standard_name="v-component"
v1000Var.units="m s-1"

tc1000Var = ncdstfile.createVariable("TC1000", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc1000Var.description="Temperature at 1000 HPa";
tc1000Var.units="C"

rh1000Var = ncdstfile.createVariable("RH1000", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh1000Var.description="Relative humidity at 1000 HPa";
rh1000Var.units="%";

u975Var = ncdstfile.createVariable("U975", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u975Var.description="grid rel. x-wind component at 975 HPa";
u975Var.standard_name="u-component"
u975Var.units="m s-1"

v975Var = ncdstfile.createVariable("V975", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v975Var.description="grid rel. y-wind component at 975 HPa";
v975Var.standard_name="v-component"
v975Var.units="m s-1"

tc975Var = ncdstfile.createVariable("TC975", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc975Var.description="Temperature at 975 HPa";
tc975Var.units="C"

rh975Var = ncdstfile.createVariable("RH975", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh975Var.description="Relative humidity at 975 HPa";
rh975Var.units="%";

u950Var = ncdstfile.createVariable("U950", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u950Var.description="grid rel. x-wind component at 950 HPa";
u950Var.standard_name="u-component"
u950Var.units="m s-1"

v950Var = ncdstfile.createVariable("V950", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v950Var.description="grid rel. y-wind component at 950 HPa";
v950Var.standard_name="v-component"
v950Var.units="m s-1"

tc950Var = ncdstfile.createVariable("TC950", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc950Var.description="Temperature at 950 HPa";
tc950Var.units="C"

rh950Var = ncdstfile.createVariable("RH950", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh950Var.description="Relative humidity at 950 HPa";
rh950Var.units="%";

u925Var = ncdstfile.createVariable("U925", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u925Var.description="grid rel. x-wind component at 925 HPa";
u925Var.standard_name="u-component"
u925Var.units="m s-1"

v925Var = ncdstfile.createVariable("V925", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v925Var.description="grid rel. y-wind component at 925 HPa";
v925Var.standard_name="v-component"
v925Var.units="m s-1"

tc925Var = ncdstfile.createVariable("TC925", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc925Var.description="Temperature at 925 HPa";
tc925Var.units="C"

rh925Var = ncdstfile.createVariable("RH925", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh925Var.description="Relative humidity at 925 HPa";
rh925Var.units="%";

u850Var = ncdstfile.createVariable("U850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u850Var.description="grid rel. x-wind component at 850 HPa";
u850Var.standard_name="u-component"
u850Var.units="m s-1"

v850Var = ncdstfile.createVariable("V850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v850Var.description="grid rel. y-wind component at 850 HPa";
v850Var.standard_name="v-component"
v850Var.units="m s-1"

kiVar = ncdstfile.createVariable("KI", "f4",("time","latitude","longitude"),fill_value=1.e+37)
kiVar.description="K-Index"
kiVar.units="C"

ttVar = ncdstfile.createVariable("TT", "f4",("time","latitude","longitude"),fill_value=1.e+37)
ttVar.description="Total Totals index"
ttVar.units="C"

tc850Var = ncdstfile.createVariable("TC850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc850Var.description="Temperature at 850 HPa";
tc850Var.units="C"

theta_e850Var = ncdstfile.createVariable("THETA_E850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
theta_e850Var.description="Equivalent Potential Temperature at 850 HPa";
theta_e850Var.units="C"

theta_w850Var = ncdstfile.createVariable("THETA_W850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
theta_w850Var.description="Wet Bulb Temperature at 850 HPa";
theta_w850Var.units="C"

delta_thetaVar = ncdstfile.createVariable("DELTA_THETA", "f4",("time","latitude","longitude"),fill_value=1.e+37)
delta_thetaVar.description="Differnce between Equivalent Potential Temperature at 500 HPa and at 850 HPa";
delta_thetaVar.units="C"

rh850Var = ncdstfile.createVariable("RH850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh850Var.description="Relative humidity at 850 HPa";
rh850Var.units="%";

u700Var = ncdstfile.createVariable("U700", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u700Var.description="grid rel. x-wind component at 700 HPa";
u700Var.standard_name="u-component"
u700Var.units="m s-1"

v700Var = ncdstfile.createVariable("V700", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v700Var.description="grid rel. y-wind component at 700 HPa";
v700Var.standard_name="v-component"
v700Var.units="m s-1"

tc700Var = ncdstfile.createVariable("TC700", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc700Var.description="Temperature at 700 HPa";
tc700Var.units="C"

rh700Var = ncdstfile.createVariable("RH700", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh700Var.description="Relative humidity at 700 HPa";
rh700Var.units="%";

u500Var = ncdstfile.createVariable("U500", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u500Var.description="grid rel. x-wind component at 500 HPa";
u500Var.standard_name="u-component"
u500Var.units="m s-1"

v500Var = ncdstfile.createVariable("V500", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v500Var.description="grid rel. y-wind component at 500 HPa";
v500Var.standard_name="v-component"
v500Var.units="m s-1"

tc500Var = ncdstfile.createVariable("TC500", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc500Var.description="Temperature at 500 HPa";
tc500Var.units="C";

rh500Var = ncdstfile.createVariable("RH500", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh500Var.description="Relative humidity at 500 HPa";
rh500Var.units="%";

u300Var = ncdstfile.createVariable("U300", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u300Var.description="grid rel. x-wind component at 300 HPa";
u300Var.standard_name="u-component"
u300Var.units="m s-1"

v300Var = ncdstfile.createVariable("V300", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v300Var.description="grid rel. y-wind component at 300 HPa";
v300Var.standard_name="v-component"
v300Var.units="m s-1"

tc300Var = ncdstfile.createVariable("TC300", "f4",("time","latitude","longitude"),fill_value=1.e+37)
tc300Var.description="Temperature at 300 HPa";
tc300Var.units="C";

rh300Var = ncdstfile.createVariable("RH300", "f4",("time","latitude","longitude"),fill_value=1.e+37)
rh300Var.description="Relative humidity at 300 HPa";
rh300Var.units="%";

gph500Var = ncdstfile.createVariable("GPH500", "f4",("time","latitude","longitude"),fill_value=1.e+37)
gph500Var.description="Geopotential height at 500 HPa";
gph500Var.units="dm";

gph850Var = ncdstfile.createVariable("GPH850", "f4",("time","latitude","longitude"),fill_value=1.e+37)
gph850Var.description="Geopotential height at 850 HPa";
gph850Var.units="dm";

slpVar = ncdstfile.createVariable("SLP", "f4",("time","latitude","longitude"),fill_value=1.e+37)
slpVar.description="Sea level pressure";
slpVar.units="HPa";

clfVar = ncdstfile.createVariable("CLDFRA_TOTAL", "f4",("time","latitude","longitude"),fill_value=1.e+37)
clfVar.description="Total cloud fraction";
clfVar.units="%";

u10mVar = ncdstfile.createVariable("U10M", "f4",("time","latitude","longitude"),fill_value=1.e+37)
u10mVar.description="grid rel. x-wind component"
u10mVar.standard_name="u-component"
u10mVar.units="m s-1"

v10mVar = ncdstfile.createVariable("V10M", "f4",("time","latitude","longitude"),fill_value=1.e+37)
v10mVar.description="grid rel. y-wind component";
v10mVar.standard_name="v-component";
v10mVar.units="m s-1";

wspd10Var = ncdstfile.createVariable("WSPD10", "f4",("time","latitude","longitude"),fill_value=1.e+37)
wspd10Var.description="wind speed at 10 meters";
wspd10Var.units="m s-1";
wspd10Var.standard_name="";

wdir10Var = ncdstfile.createVariable("WDIR10", "f4",("time","latitude","longitude"),fill_value=1.e+37)
wdir10Var.description="wind dir at 10 meters";
wdir10Var.units="nord degrees";
wdir10Var.standard_name="";

dwspd10Var = ncdstfile.createVariable("DELTA_WSPD10", "f4",("time","latitude","longitude"),fill_value=1.e+37)
dwspd10Var.description="Difference of wind speed at 10 meters";
dwspd10Var.units="m s-1";
dwspd10Var.standard_name="";

dwdir10Var = ncdstfile.createVariable("DELTA_WDIR10", "f4",("time","latitude","longitude"),fill_value=1.e+37)
dwdir10Var.description="Difference of wind dir at 10 meters";
dwdir10Var.units="nord degrees";
dwdir10Var.standard_name="";

timeVar[:]=date2num(dates,units=timeVar.units)
lonVar[:]=lons
latVar[:]=lats
dwspd10Var[0]= dwspd10i
dwdir10Var[0]= dwdir10i
drainVar[0]  = draini
hrainVar[0]  = hraini
hsweVar[0]    = hswei
pwVar[0]    = interp(Xlon,Xlat,pw,lons,lats)
rh2Var[0]    = interp(Xlon,Xlat,rh2,lons,lats)
t2cVar[0]    = interp(Xlon,Xlat,t2c[0],lons,lats)
uhVar[0]     = interp(Xlon,Xlat,updraft_helicity,lons,lats)
srhVar[0]      = interp(Xlon,Xlat,helicity,lons,lats)
mcapeVar[0]  = interp(Xlon,Xlat,cape_2d[0],lons,lats)
mcinVar[0]  = interp(Xlon,Xlat,cape_2d[1],lons,lats)

u1000Var[0]  = interp(Xlon,Xlat,u1000,lons,lats)
v1000Var[0]  = interp(Xlon,Xlat,v1000,lons,lats)
tc1000Var[0]  = interp(Xlon,Xlat,tc1000,lons,lats)
rh1000Var[0]  = interp(Xlon,Xlat,rh1000,lons,lats)

u975Var[0]  = interp(Xlon,Xlat,u975,lons,lats)
v975Var[0]  = interp(Xlon,Xlat,v975,lons,lats)
tc975Var[0]  = interp(Xlon,Xlat,tc975,lons,lats)
rh975Var[0]  = interp(Xlon,Xlat,rh975,lons,lats)

u950Var[0]  = interp(Xlon,Xlat,u950,lons,lats)
v950Var[0]  = interp(Xlon,Xlat,v950,lons,lats)
tc950Var[0]  = interp(Xlon,Xlat,tc950,lons,lats)
rh950Var[0]  = interp(Xlon,Xlat,rh950,lons,lats)

u925Var[0]  = interp(Xlon,Xlat,u925,lons,lats)
v925Var[0]  = interp(Xlon,Xlat,v925,lons,lats)
tc925Var[0]  = interp(Xlon,Xlat,tc925,lons,lats)
rh925Var[0]  = interp(Xlon,Xlat,rh925,lons,lats)

u850Var[0]  = interp(Xlon,Xlat,u850,lons,lats)
v850Var[0]  = interp(Xlon,Xlat,v850,lons,lats)
tc850Var[0]  = interp(Xlon,Xlat,tc850,lons,lats)
rh850Var[0]  = interp(Xlon,Xlat,rh850,lons,lats)
ttVar[0]  = interp(Xlon,Xlat,tt,lons,lats)
kiVar[0]  = interp(Xlon,Xlat,ki,lons,lats)
theta_e850Var[0]  = interp(Xlon,Xlat,theta_e850,lons,lats)
theta_w850Var[0]  = interp(Xlon,Xlat,theta_w850,lons,lats)
delta_thetaVar[0]  = interp(Xlon,Xlat,delta_theta,lons,lats)

u700Var[0]  = interp(Xlon,Xlat,u700,lons,lats)
v700Var[0]  = interp(Xlon,Xlat,v700,lons,lats)
tc700Var[0]  = interp(Xlon,Xlat,tc700,lons,lats)
rh700Var[0]  = interp(Xlon,Xlat,rh700,lons,lats)

u500Var[0]  = interp(Xlon,Xlat,u500,lons,lats)
v500Var[0]  = interp(Xlon,Xlat,v500,lons,lats)
tc500Var[0]  = interp(Xlon,Xlat,tc500,lons,lats)
rh500Var[0]  = interp(Xlon,Xlat,rh500,lons,lats)

u300Var[0]  = interp(Xlon,Xlat,u300,lons,lats)
v300Var[0]  = interp(Xlon,Xlat,v300,lons,lats)
tc300Var[0]  = interp(Xlon,Xlat,tc300,lons,lats)
rh300Var[0]  = interp(Xlon,Xlat,rh300,lons,lats)

gph500Var[0] = interp(Xlon,Xlat,gph500,lons,lats)
gph850Var[0] = interp(Xlon,Xlat,gph850,lons,lats)
slpVar[0]    = interp(Xlon,Xlat,slp,lons,lats)
clfVar[0]    = interp(Xlon,Xlat,clf,lons,lats)
u10mVar[0]   = interp(Xlon,Xlat,uvmet10[0],lons,lats)
v10mVar[0]   = interp(Xlon,Xlat,uvmet10[1],lons,lats)
wspd10Var[0] = wspd10i
wdir10Var[0] = wdir10i
ncdstfile.close()

ncsrcfile.close()
