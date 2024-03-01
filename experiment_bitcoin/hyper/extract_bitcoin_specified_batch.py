import requests
import json
import file_property
import subprocess
import time
import os
import sys
import shutil
import bitcoin2graphson
import multiprocessing



# directory
property_dir = "/public/home/blockchain/master/experiment_bitcoin/hyper/"
# parameters
rpc_url = "127.0.0.1:8332"
rpc_user = "root"
rpc_password = "root"
method1 = "getblockhash"
method2 = "getblock"
params_list = []
bithash_list = []

windows_memory = 10000
processes = []
print(time.time())
# initialization
def init(property_dir):
    #subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
    #"connect to server: unknown"
    shutil.rmtree("/public/home/blockchain/master/experiment_bitcoin/hyper/bitcoin_graphson")
    os.mkdir("/public/home/blockchain/master/experiment_bitcoin/hyper/bitcoin_graphson")
    print("Graphson-dir ready!")
    property_file = property_dir + "bitcoin_data.properties"
    open(property_file, 'w').close()
    content = file_property.parse(property_file)
    content.put("bitcoin_block_height", "0")
    content.put("incre_status", "stop")
    property_file = property_dir + "bitcoin_loader.properties"
    open(property_file, 'w').close()
    content = file_property.parse(property_file)
    content.put("current_status", "start")
    content.put("current_json_transform", "0")
    content.put("incre_status", "stop")
    while True:
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        # err=result.stderr.decode("utf-8")
        # print(err)
        if "Could not connect to the server" in err:
            print("Bitcoind stopped")
            break
    try:
        print("kill 8333")
        result = subprocess.run("lsof -i :8333", shell=True, capture_output=True, text=True)
        print(result)
        lines = result.stdout.split("\n")
        line = lines[1]
        fileds = line.split()
        pid = fileds[1]
        command = "kill -9 " + str(pid)
        print(command)
        result = subprocess.run(command, shell=True)
    except:
        pass
    subprocess.run("rm -r /public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp", shell=True, capture_output=True)
    if os.path.exists("/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp") and os.path.isdir("/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp"):
        print("Folder deletion error")
        sys.exit()
    print("Folder deletion completed!")
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    subprocess.run("mkdir /public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp", shell=True, capture_output=True)
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    result = subprocess.run("touch /public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp/bitcoin.conf", shell=True, capture_output=True)
    #out=result.stdout.decode("utf-8")
    #err=result.stderr.decode("utf-8")
    #print(out,err)
    property_file = "/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp/bitcoin.conf"
    content = file_property.parse(property_file)
    content.put("listen", "1")
    content.put("server", "1")
    content.put("rpcuser", "root")
    content.put("rpcpassword", "root")
    content.put("rpcport", "8332")
    # content.put("rpcbind", "0.0.0.0")
    content.put("prune", str(windows_memory * 4 * 4))
    #while True:
    result = subprocess.run("nohup /public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoind -datadir=/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp &", shell=True)
    file_path = property_dir + "data_tmp/debug.log"
    while True:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    pass  # 迭代文件内容，最终将指向文件的最后一行
            if "DNS" in line:
                break
            else:
                # print(line)
                continue
        except:
            continue
    while True:
        #out=result.stdout.decode("utf-8")
        #err=result.stderr.decode("utf-8")
        #print(out,err)
        #print("here")
        #while True:
        # time.sleep(5)
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli -getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        if "version" in out:
            print("initialization completed!")
            break
        else:
            print(out)
            print(err)

def bitcoin_rpc(rpc_url, rpc_user, rpc_password, method, params_list):
    headers = {'content-type': 'application/json'}
    payload = []
    #print(params_list)
    #print(params_list[-1])
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": method, "params": params_list[-1]})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    while True:
        try:
            response = requests.post(url, headers=headers, data=data)
            result = response.json()
            return result
        except:
            pass
    
def get_parameters(windows_memory, block_num):
    params_list = []
    # property_file = property_dir + "bitcoin_data.properties"
    # content = file_property.parse(property_file)
    # block_num = int(content.get("bitcoin_block_height"))
    for i in range(windows_memory):
        param_block_height = block_num+i
        param_list = [param_block_height]
        params_list.append(param_list)
    return params_list

def get_block_time(params_list):
    bithash_list = []
    bithash_result = bitcoin_rpc(rpc_url, rpc_user, rpc_password, method1, params_list)
    bithash = [str(bithash_result[0]['result']), 2]
    bithash_list.append(bithash)
    #print(params_list[-1],block_num,windows_memory,block_height)
    result = bitcoin_rpc(rpc_url, rpc_user, rpc_password, method2, bithash_list)
    # print(result)
    block_time = result[0]['result']['time']
    return block_time

def get_current_height(rpc_url, rpc_user, rpc_password):
    headers = {'content-type': 'application/json'}
    payload = []
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getblockchaininfo", "params": []})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    while True:
        try:
            response = requests.post(url, headers=headers, data=data)
            result = response.json()
            block_height = result[0]['result']['blocks']
            return block_height
        except:
            pass

def restart_client_stopS():
    while True:
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        # err=result.stderr.decode("utf-8")
        print(err,"here")
        if "Could not connect to the server" in err:
            break
    
    file_path = property_dir + "data_tmp/debug.log"
    while True:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                pass
        if "Shutdown: done" in line:
            break
    result = subprocess.run("nohup /public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoind -datadir=/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp -noconnect &", shell=True)
    while True:
        # print("11111")
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli -getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        print(out,err)
        if "version" in out:
            break

def restart_client_startS():
    while True:
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        # err=result.stderr.decode("utf-8")
        print(err)
        if "Could not connect to the server" in err:
            break
    file_path = property_dir + "data_tmp/debug.log"
    while True:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                pass  # 迭代文件内容，最终将指向文件的最后一行
        if "Shutdown: done" in line:
            break
    result = subprocess.run("nohup /public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoind -datadir=/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp &", shell=True)
    while True:
        # print("11111111")
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli -getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        print(out, err)
        if "version" in out:
            break


def stop_client():
    while True:
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        # err=result.stderr.decode("utf-8")
        print(err)
        if "Could not connect to the server" in err:
            break
    
    file_path = property_dir + "data_tmp/debug.log"
    while True:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                pass
        if "Shutdown: done" in line:
            break

def start_client():
    result = subprocess.run("nohup /public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoind -datadir=/public/home/blockchain/master/experiment_bitcoin/hyper/data_tmp &", shell=True)
    file_path = property_dir + "data_tmp/debug.log"
    while True:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    pass  # 迭代文件内容，最终将指向文件的最后一行
            if "UpdateTip" in line:
                break
        except:
            continue
    while True:
        # print("11111")
        result = subprocess.run("/public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoin-cli -getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        # print(out,err)
        if "version" in out:
            break



def parse(params_list,windows_memory):
    bitcoin2graphson.bitcoin_to_graphson(params_list,windows_memory)



if __name__ == "__main__":
    start_time = int(time.time())
    init(property_dir)
    tmp_block = 424
    while True:
        #print(len(processes))
        #time.sleep(50)
        try:
            property_file = property_dir + "bitcoin_data.properties"
            content = file_property.parse(property_file)
            block_num = int(content.get("bitcoin_block_height"))
        except:
            continue
        block_height = get_current_height(rpc_url, rpc_user, rpc_password)
        # print(block_height,block_num)
        if (block_height >= 1000) or (block_height >= block_num + windows_memory and block_height <= block_num + 3 * windows_memory):
            params_list = get_parameters(1000, block_num)
            block_time = get_block_time(params_list)
            # print(block_time)
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
                    restart_client_startS()
                    print("stop!")
                    while True:
                        remove_list = []
                        for process in processes:
                            if not process.is_alive():
                                remove_list.append(process)
                                process.terminate()
                        for remove_process in remove_list:
                            processes.remove(remove_process)
                        if len(processes) < 10:
                            restart_client_stopS()
                            print("start!")
                            break
                p = multiprocessing.Process(target=parse, args=(params_list,1000,))
                p.start()
                processes.append(p)
                tmp_block = block_num
        elif block_height < block_num + windows_memory:
            continue
        elif block_height > block_num + 3 * windows_memory:
            restart_client_stopS()
            while True:
                params_list = get_parameters(1000, block_num)
                block_time = get_block_time(params_list)
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
                    p = multiprocessing.Process(target=parse, args=(params_list,1000,))
                    p.start()
                    processes.append(p)
                    tmp_block = block_num
                while True:
                    try:
                        content = file_property.parse(property_file)
                        block_num = int(content.get("bitcoin_block_height"))
                        block_height = get_current_height(rpc_url, rpc_user, rpc_password)
                        break
                    except:
                        continue
                #print(block_height,block_num)
                if block_num >= 1000:
                    break
                if block_height <= block_num + 3 * windows_memory:
                    break
            restart_client_startS()

        # when to stop
        #print(block_time,start_time)
        if block_time >= start_time or block_num >= 1000 - windows_memory:
            restart_client_stopS()
            for process in processes:
                if process.is_alive():
                    process.join()
            while True:
                try:
                    property_file = property_dir + "bitcoin_loader.properties"
                    content = file_property.parse(property_file)
                    content.put("current_status", "stop")
                    break
                except:
                    continue
            break
