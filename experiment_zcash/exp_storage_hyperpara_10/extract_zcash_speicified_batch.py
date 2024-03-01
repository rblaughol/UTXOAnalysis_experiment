import requests
import json
import file_property
import subprocess
import time
import multiprocessing
import zcashtographson 
import shutil
import os
import paramiko
from scp import SCPClient
# rpc in this file, which is stable currently
# directory
property_dir = "/home/slave15/experiment/exp_storage_hyperpara_10/"
# parameters
rpc_url = "127.0.0.1:8232"
rpc_user = "root"
rpc_password = "root"
method = "getblock"
params_list = []

windows_memory = 100000
processes = []
print(time.time())

# initialization
def init(property_dir):
    #subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
    # avoid the wrong data
    shutil.rmtree('/home/slave15/zcash_graphson')
    os.mkdir('/home/slave15/zcash_graphson')
    shutil.rmtree('/home/slave15/tmp')
    os.mkdir('/home/slave15/tmp')
    # modify the data file
    property_file = property_dir + "zcash_data.properties"
    open(property_file, 'w').close()
    content = file_property.parse(property_file)
    content.put("zcash_block_height", "0")
    content.put("incre_status", "stop")
    #content.put("extract_height","0")
    property_file = property_dir + "zcash_loader.properties"
    open(property_file, 'w').close()
    content = file_property.parse(property_file)
    content.put("current_status", "start")
    content.put("current_json_transform", "0")
    content.put("incre_status","stop")
    # avoid the remote wrong data
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('192.168.31.1', username='master', password='Ppsuc@gu2024')
    command = 'rm -rf /public/home/blockchain/master/zcash_graphson'
    ssh.exec_command(command)

    # in order to avoid operation error
    while True:
        command = 'ls /public/home/blockchain/master/'
        stdin, stdout, stderr = ssh.exec_command(command)
        output = ''.join(stdout.readlines())
        #print(output)
        if "zcash_graphson" not in output:
            break
    command = 'mkdir /public/home/blockchain/master/zcash_graphson'
    ssh.exec_command(command)
    ssh.close()
    # the flag which control the status of loader
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('192.168.31.1', username='master', password='Ppsuc@gu2024')
    scp = SCPClient(ssh.get_transport())
    local_file_path = '/home/slave15/experiment/exp_storage_hyperpara_10/zcash_loader.properties'
    remote_file_path = '/public/home/blockchain/master/experiment/exp_storage_hyperpara_10/'
    scp.put(local_file_path, remote_file_path)
    ssh.close()
    scp.close()

    while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(err)
        if "connect to server: unknown" in err:
            break

    subprocess.run("rm -r /home/slave15/data_tmp", shell=True, capture_output=True)
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    subprocess.run("mkdir /home/slave15/data_tmp", shell=True, capture_output=True)
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    result = subprocess.run("touch /home/slave15/data_tmp/zcash.conf", shell=True, capture_output=True)
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    property_file = "/home/slave15/data_tmp/zcash.conf"
    content = file_property.parse(property_file)
    content.put("listen", "1")
    content.put("server", "1")
    content.put("rpcuser", "root")
    content.put("rpcpassword", "root")
    content.put("server", "1")
    content.put("rpcport", "8232")
    content.put("rpcconnect", "127.0.0.1")
    content.put("prune", str(windows_memory*4))
    while True:
        result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp &", shell=True)
        #out=result.stdout.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(out,err)
        #print("here")
        #while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        if "version" in out:
            break

def zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list):
    headers = {'content-type': 'application/json'}
    payload = []
    #print(params_list)
    #print(params_list[-1])
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": method, "params": params_list[-1]})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    response = requests.post(url, headers=headers, data=data)
            
    if response.status_code == 200:
        try:
            result = response.json()
            #property_file = property_dir + "zcash_data.properties"
            #content = file_property.parse(property_file)
            #block_num = int(content.get("zcash_block_height"))
            #block_num += windows_memory
            #content.put("zcash_block_height", str(block_num))
            #time.sleep(10)
            return result
        except ValueError as e:
            print("error: Invalid JSON response from server")
            zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list)
    else:
        print("error" + f"HTTP error {response.status_code}")
        zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list)

def get_parameters(windows_memory, block_num):
    params_list = []
    #property_file = property_dir + "zcash_data.properties"
    #content = file_property.parse(property_file)
    #block_num = int(content.get("zcash_block_height"))
    for i in range(windows_memory):
        param_block_height = str(block_num+i)
        param_list = [param_block_height,2]
        params_list.append(param_list)
    return params_list
#params_list = [["140000",2], ["140001",2]]  

def get_current_height(rpc_url, rpc_user, rpc_password):
    headers = {'content-type': 'application/json'}
    payload = []
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getinfo", "params": []})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    response = requests.post(url, headers=headers, data=data)
        
    if response.status_code == 200:
        try:
            result = response.json()
            # print(result)
            block_height = result[0]['result']['blocks']
            return block_height
        except ValueError as e:
            print("{'error': 'Invalid JSON response from server'}")
            get_current_height(rpc_url, rpc_user, rpc_password)
    else:
        print("error: " + f"HTTP error {response.status_code}")
        get_current_height(rpc_url, rpc_user, rpc_password)

def restart_client_stopS():
    while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(err)
        if "connect to server: unknown" in err:
            break
    while True:
        result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp -connect=0 &", shell=True)
        # while True:
        #print("11111")
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        #print(out,err)
        if "version" in out:
            break

def restart_client_startS():
    while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(err)
        if "connect to server: unknown" in err:
            break
    while True:
        result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp &", shell=True)
        #while True:
        #print("11111111")
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        if "version" in out:
            break

def stop_client():
    while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(err)
        if "connect to server: unknown" in err:
            break

def start_client():
    while True:
        result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp &", shell=True)
        #while True:
        #print("11111111")
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        if "version" in out:
            break

def parse(params_list,windows_memory):
    zcashtographson.zcash_to_graphson(params_list,windows_memory)

if __name__ == "__main__":
    #print(time.time())
    #print(time.time())
    start_time = int(time.time())
    init(property_dir)
    tmp_block = 424
    while True:
        #print(len(processes))
        #time.sleep(50)
        try:
            property_file = property_dir + "zcash_data.properties"
            content = file_property.parse(property_file)
            block_num = int(content.get("zcash_block_height"))
        except:
            continue
        block_height = get_current_height(rpc_url, rpc_user, rpc_password)
        #print(block_height,block_num)
        if (block_height >= 10000) or (block_height >= block_num + windows_memory and block_height <= block_num + 3 * windows_memory):
        #if block_height >= block_num + windows_memory and block_height <= block_num + 3 * windows_memory:
            params_list = get_parameters(10000, block_num)
            #params_list = get_parameters(windows_memory, block_num)
            #print(params_list[-1],block_num,windows_memory,block_height)
            result = zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list)
            #print(result)
            block_time = result[0]['result']['time']
            if block_num != tmp_block:
                # remove the process
                remove_list = []
                for process in processes:
                    if not process.is_alive():
                        remove_list.append(process)
                        process.terminate()
                for remove_process in remove_list:
                    processes.remove(remove_process)
                # identify whether the length of processes is more tahan 10
                if len(processes) > 10:
                    restart_client_stopS()
                    print("stop!")
                    while True:
                        print(processes)
                        remove_list = []
                        for process in processes:
                            if not process.is_alive():
                                remove_list.append(process)
                                process.terminate()
                        for remove_process in remove_list:
                            processes.remove(remove_process)
                        if len(processes) < 10:
                            restart_client_startS()
                            print("start!")
                            break
                p = multiprocessing.Process(target=parse, args=(params_list,10000,))
                #p = multiprocessing.Process(target=parse, args=(params_list,windows_memory,))
                p.start()
                processes.append(p)
                tmp_block = block_num
        elif block_height < block_num + windows_memory:
            continue
        elif block_height > block_num + 3 * windows_memory:
            restart_client_stopS()
            while True:
                #print("222222222")
                params_list = get_parameters(10000,block_num)
                #params_list = get_parameters(windows_memory,block_num)
                result = zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list)
                print(block_num,tmp_block)
                block_time = result[0]['result']['time']
                #print(block_height,block_num)
                if block_num != tmp_block:
                    # remove the process
                    remove_list = []
                    for process in processes:
                        if not process.is_alive():
                            remove_list.append(process)
                            process.terminate()
                    for remove_process in remove_list:
                        processes.remove(remove_process)
                    # identify whether the length of processes is more than 10
                    if len(processes) > 10:
                        print("stop~")
                        while True:
                            remove_list = []
                            for process in processes:
                                if not process.is_alive():
                                    remove_list.append(process)
                                    process.terminate()
                            for remove_process in remove_list:
                                processes.remove(remove_process)
                            if len(processes) < 10:
                                print("start~")
                                break
                    p = multiprocessing.Process(target=parse, args=(params_list,10000,))
                    #p = multiprocessing.Process(target=parse, args=(params_list,windows_memory,))
                    p.start()
                    processes.append(p)
                    tmp_block = block_num
                while True:
                    try:
                        content = file_property.parse(property_file)
                        block_num = int(content.get("zcash_block_height"))
                        block_height = get_current_height(rpc_url, rpc_user, rpc_password)
                        break
                    except:
                        continue
                #print(block_height,block_num)
                if block_num >= 10000:
                    break
                if block_height <= block_num + 3 * windows_memory:
                    break
            restart_client_startS()

        # when to stop
        #print(block_time,start_time)
        print(block_num)

        if block_time >= start_time or block_num >= 10000 - windows_memory:
            print(processes)
            for process in processes:
                if process.is_alive():
                    process.join()
            restart_client_stopS()
            while True:
                try:
                    property_file = property_dir + "zcash_loader.properties"
                    content = file_property.parse(property_file)
                    content.put("current_status", "stop")
                    break
                except:
                    continue
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect('192.168.31.1', username='master', password='Ppsuc@gu2024')
            scp = SCPClient(ssh.get_transport())
            local_file_path = '/home/slave15/experiment/exp_storage_hyperpara_10/zcash_loader.properties'
            remote_file_path = '/public/home/blockchain/master/experiment/exp_storage_hyperpara_10/'
            scp.put(local_file_path, remote_file_path)
            ssh.close()
            scp.close()
            break
