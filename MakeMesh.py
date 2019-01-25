# coding: utf-8
import glob
import os
import time
from jpmesh import Angle, Coordinate, FirstMesh, SecondMesh, ThirdMesh, HalfMesh, QuarterMesh, OneEighthMesh
try:
    import cPickle as pickle
except:
    import pickle
from multiprocessing import Process

def run(file_dir_list):
    def summary(mesh_hash, mesh_code, summary_value_dict):
        if mesh_code in mesh_hash.keys():
            mesh_hash[mesh_code]["count"] += 1
            for summary_val in summary_value_list:
                mesh_hash[mesh_code]["sum"][summary_val] += summary_value_dict[summary_val]
            call_result = summary_value_dict["call_result"]
            if call_result.upper() == "OK":
                mesh_hash[mesh_code]["sum"]["Ok"] += 1
            elif call_result.upper() == "FAIL":
                mesh_hash[mesh_code]["sum"]["Fail"] += 1
            elif call_result.upper() == "DROP":
                mesh_hash[mesh_code]["sum"]["Drop"] += 1
        else:
            mesh_hash[mesh_code] = {"count" : 1, "sum" : {}}
            for summary_val in summary_value_list:
                mesh_hash[mesh_code]["sum"][summary_val] = summary_value_dict[summary_val]
            mesh_hash[mesh_code]["sum"]["Ok"] = 0
            mesh_hash[mesh_code]["sum"]["Fail"] = 0
            mesh_hash[mesh_code]["sum"]["Drop"] = 0
            call_result = summary_value_dict["call_result"]
            if call_result.upper() == "OK":
                mesh_hash[mesh_code]["sum"]["Ok"] = 1
            elif call_result.upper() == "FAIL":
                mesh_hash[mesh_code]["sum"]["Fail"] = 1
            elif call_result.upper() == "DROP":
                mesh_hash[mesh_code]["sum"]["Drop"] = 1
    
    def make_mesh_hash(coordinate, mesh_hash, summary_value_dict):
        mesh = HalfMesh.from_coordinate(coordinate) # change
        summary(mesh_hash, mesh.code, summary_value_dict)
    
    summary_value_list = ["rsrp", "rsrq", "sinr", "cqi", "thp_dl", "thp_ul", "PCI", "cell_max_tx_power",
    "traffic_load", "rs_power"]
    for file_dir in file_dir_list:
        line_read_cmd = "cat %s | wc -l" % file_dir
        full_line = os.popen(line_read_cmd).read().strip()

        print "full line : ", full_line

        fail_line = []
        line = 0

        mesh_hash = {}

        s_time = time.time()
        with open(file_dir, "r") as f:
            print file_dir
            col_schema = f.readline().strip().split(",")
            for origin in f:
                column_dict = {}
                origin = origin.replace("\"", "").strip().split(",")
                for idx, item in enumerate(origin):
                    column_dict[col_schema[idx]] = item
                date_time = column_dict["dt"].split(".")[0]
                summary_value_dict = {}
                try:
                    summary_value_dict["rsrp"] = float(column_dict["rsrp"].strip())
                    summary_value_dict["rsrq"] = float(column_dict["rsrq"].strip())
                    summary_value_dict["sinr"] = float(column_dict["sinr"].strip())
                    summary_value_dict["cqi"] = float(column_dict["cqi"].strip())
                    if column_dict["thp_dl"].strip() == "":
                        summary_value_dict["thp_dl"] = 0
                    else:
                        summary_value_dict["thp_dl"] = float(column_dict["thp_dl"].strip())
                    if column_dict["thp_ul"].strip() == "":
                        summary_value_dict["thp_ul"] = 0
                    else:
                        summary_value_dict["thp_ul"] = float(column_dict["thp_ul"].strip())
                    summary_value_dict["PCI"] = float(column_dict["PCI"].strip())
                    summary_value_dict["cell_max_tx_power"] = float(column_dict["cell_max_tx_power"].strip())
                    summary_value_dict["traffic_load"] = float(column_dict["traffic_load"].strip())
                    summary_value_dict["rs_power"] = float(column_dict["rs_power"].strip())
                    summary_value_dict["call_result"] = column_dict["call_result"].strip()
                except Exception, e:
                    print e
                    print "thp_ul: ", column_dict["thp_ul"]
                    print "thp_dl: ", column_dict['thp_dl']

                _lon = float(column_dict["lon"])
                _lat = float(column_dict["lat"])
                try:
                    coordinate = Coordinate(lon=Angle.from_degree(_lon), lat=Angle.from_degree(_lat))
                    make_mesh_hash(coordinate, mesh_hash, summary_value_dict)
                except Exception, err:
                    print err
                    fail_line.append(line)
                line += 1
                #print "%s / %s" % (line, full_line)
        e_time = time.time()
        print "time : %d" % (e_time - s_time)
        print len(fail_line)

        result_dict = mesh_hash
        for mesh in result_dict.keys():
            result_dict[mesh]["avg"] = {}
            count = result_dict[mesh]["count"]
            for val in result_dict[mesh]["sum"].keys():
                result_dict[mesh]["avg"][val] = result_dict[mesh]["sum"][val] / count
        
        with open("./mesh_data/" + file_dir[len(mypath):] + "_result.txt", "wb") as f:
            pickle.dump(result_dict, f)

def Main():
    mypath = "/DATA5/iris2/201901/"
    files = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f)) and f.endswith(".csv")]
    file_list = []
    for file in files:
        os.path.join(mypath, file)
        file_list.append(os.path.join(mypath, file))
    max_p_cnt = 10
    job_total = len(file_list)
    job_per_process = job_total/max_p_cnt
    job_list = []
    p_list = []
    for file_dir in file_list:
        job_list.append(file_dir)
        if len(job_list) >= job_per_process:
            p = Process(target=run, args=(job_list,))
            p.start()
            p_list.append(p)
            job_list = []
    for p in p_list:
        p.join()

if __name__ == "__main__":
    Main()
