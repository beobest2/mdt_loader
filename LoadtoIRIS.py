import API.M6 as M6
import os
import time

def Main():
    conn = M6.Connection("192.168.100.180:5050", "mdt", "123123", Database="mdt")
    c = conn.Cursor()
    c.SetFieldSep('|^|')
    c.SetRecordSep('\n')
    
    fail_cnt = 0
    mypath = "./dat_file"
    files = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f)) and f.endswith(".dat")]
    file_list = []
    for file in files:
        os.path.join(mypath, file)
        file_list.append(os.path.join(mypath, file))
    for idx, file in enumerate(file_list):
        parse_list = files[idx][:-4].split("_")
        partition_date = parse_list[0]
        partition_key = "_".join([parse_list[1], parse_list[2]])
        print partition_date
        print partition_key
        load_str = c.Load('JP_MESH_SUMMARY', partition_key, partition_date, 'control_file.ctl', file)
        if load_str.strip().startswith("+OK"):
            pass
        else:
            fail_cnt += 1
        print load_str
    
    c.Close()
    conn.close()
    print "fail cnt : ", fail_cnt

if __name__ == "__main__":
    Main()
