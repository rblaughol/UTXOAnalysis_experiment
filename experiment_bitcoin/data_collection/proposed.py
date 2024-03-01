import requests
import json
import file_property
import subprocess
import time
import os
import sys
import shutil
import multiprocessing

# directory
property_dir = "/public/home/blockchain/master/experiment_bitcoin/data_collection/"

def init(property_dir):

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
    subprocess.run("rm -r /public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp", shell=True, capture_output=True)
    if os.path.exists("/public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp") and os.path.isdir("/public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp"):
        print("Folder deletion error")
        sys.exit()
    subprocess.run("mkdir /public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp", shell=True, capture_output=True)
    result = subprocess.run("touch /public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp/bitcoin.conf", shell=True, capture_output=True)
    property_file = "/public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp/bitcoin.conf"
    content = file_property.parse(property_file)
    content.put("listen", "1")
    content.put("server", "1")
    content.put("rpcuser", "root")
    content.put("rpcpassword", "root")
    content.put("rpcport", "8332")
    content.put("prune", str(600))
    result = subprocess.run("nohup /public/home/blockchain/master/bitcoin-0.21.1/bin/bitcoind -datadir=/public/home/blockchain/master/experiment_bitcoin/data_collection/data_tmp &", shell=True)


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
            
def batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method, params_list):
    headers = {'content-type': 'application/json'}
    payload = []
    for params in params_list:
        payload.append({"jsonrpc": "1.0", "id": "curltest", "method": method, "params": params})

    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    #response = requests.post(url, headers=headers, data=data)
    while True:
        try:
            response = requests.post(url, headers=headers, data=data)
            result = response.json()
            #print(result)
            if method == 'getblockhash':
                # print('blockhash')
                if len(result[-1]['result']) == 64:
                    return result
                elif params == [1]:
                    time.sleep(50)
            else:
                if result[0]['result']['height']:
                    pass
                return result
        except:
            if params == [1]:
                time.sleep(50)
            continue

if __name__ == "__main__":
    print(time.time())
    a = 0
    init(property_dir)
    for i in range(int(100000/5000)):
        if i == 0:
            time.sleep(100)
            # a = time.time()
        rpc_url = "127.0.0.1:8332"
        rpc_user = "root"
        rpc_password = "root"
        method1 = "getblockhash"
        method2 = "getblock"
        
        params_list = []
        for j in range(5000):
            param_block_height = j+i*5000
            param_list = [param_block_height]
            params_list.append(param_list)
       
        bithash_result = batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method1, params_list)
        b = time.time()
        #print(bithash_result)
        bithash_list = []
        for hash_result in bithash_result:
            bithash = [str(hash_result['result']), 2]
            bithash_list.append(bithash)
        blocks = batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method2, bithash_list)
        #print(blocks)
        c = time.time()
        a += c - b
        #bithash_result = batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method1, [i])
        #bithash = [str(bithash_result[0]['result']), 2]
        #blocks= batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method2, bithash)
        print(i)
    print(a)