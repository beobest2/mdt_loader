# coding: utf-8
import glob
import os
import time
import json
from jpmesh import Angle, Coordinate, FirstMesh, SecondMesh, ThirdMesh, parse_mesh_code
try:
    import cPickle as pickle
except:
    import pickle

field_sep = "|^|"
record_sep = "\n"

def make_min_max_lat_lon(mesh):
    start_lat = mesh.south_west.lat.degree
    start_lon = mesh.south_west.lon.degree 
    end_lat = mesh.south_west.lat.degree + mesh.size.lat.degree
    end_lon = mesh.south_west.lon.degree + mesh.size.lon.degree
    return(start_lat, start_lon, end_lat, end_lon)

def make_polygon_string(code):
    mesh = parse_mesh_code(code)
    start_lat, start_lon, end_lat, end_lon = make_min_max_lat_lon(mesh)
    polygon_string = "POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"\
                            .format(xmin=start_lat, xmax=end_lat, ymin=start_lon, ymax=end_lon)
    corr_list = [[[start_lat, start_lon], [end_lat, start_lon], [end_lat, end_lon], [start_lat, end_lon], [start_lat, start_lon]]]
    geo_json_dict = {"type": "Polygon", "coordinates": corr_list}
    geo_json = json.dumps(geo_json_dict)
    return(polygon_string, geo_json)

def make_value_list(avg_dict, sum_dict):
    value_list = []
    key_str_list = ["rsrp", "rsrq", "sinr", "cqi", "thp_dl", "thp_ul", "PCI", "cell_max_tx_power", "traffic_load", "rs_power"]
    for k in key_str_list:
        value_list.append(avg_dict[k])
    key_str_list = ["Ok", "Fail", "Drop"]
    for k in key_str_list:
        value_list.append(sum_dict[k])
    return map(str, value_list)

def Main():
    file_dir = "/DATA5/iris2/mdt_script_hw/mesh_data"
    files = [f for f in os.listdir(file_dir) if os.path.isfile(os.path.join(file_dir, f)) and f.endswith(".txt")]
    file_list = []
    for file in files:
        os.path.join(file_dir, file)
        file_list.append(os.path.join(file_dir, file))
    
    block_max_rows = 1000000
    mesh_level = 3 # change
    for file in file_list:
        with open(file_list[0], "rb") as f:
            mesh_hash = pickle.load(f)
        partition_date_str = file.split("/")[-1][:11].replace("_","") + "0000"
    
        block_row = 0
        block_cnt = 0
        for mesh_code in mesh_hash.keys():
            if block_row > block_max_rows:
                block_row = 0
                block_cnt += 1
            partition_key_str = "K%s_%s" % (mesh_level, block_cnt)
            block_row += 1
            print mesh_code
            polygon_str, geo_json_str = make_polygon_string(mesh_code)
            avg_dict = mesh_hash[mesh_code]["avg"]
            sum_dict = mesh_hash[mesh_code]["sum"]
            value_list = make_value_list(avg_dict, sum_dict)
            load_list = [partition_date_str, partition_key_str, str(mesh_level), mesh_code, geo_json_str, polygon_str] + value_list
            with open("dat_file/%s_%s.dat" % (partition_date_str, partition_key_str), "a") as f:
                f.write(field_sep.join(load_list) + record_sep)

if __name__ == "__main__":
    Main()
