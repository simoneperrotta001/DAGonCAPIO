import sys
import Render

data_file = ""
result_file = ""
output = ""

if len(sys.argv) != 4:
    print("data_file output")
    print("NB: data_file named as path/modl_dom_yyyymmddZhh00.nc es. path/wrf5_d01_20231121Z1200.nc")
    sys.exit(-1)

data_file = sys.argv[1]
output = sys.argv[2]
result_file = sys.argv[3]

fname = "archive2plot.conf"
config = {}
with open(fname) as f:
    content = f.readlines()
    for line in content:
        line = line.replace("\n", "").replace("\r", "")
        if line == "" or line.startswith('#') or not " = " in line:
            continue

        parts = line.split(" = ")

        if '"' in parts[1][0] and '"' in parts[1][-1:]:
            config[parts[0]] = parts[1].replace('"', '')
        else:
            if '.' in parts[1]:
                config[parts[0]] = float(parts[1])
            else:
                config[parts[0]] = int(parts[1])

render = Render.Render(config)
render.plotter(data_file, output, result_file)

