import time
import os
import paramiko
import hashlib
from scp import SCPClient
import requests
import json
import subprocess
import multiprocessing
import shutil
import file_property

memory = "4000"

def init():
    while True:
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
        err=result.stderr.decode("utf-8")
        if "connect to server: unknown" in err:
            break
    subprocess.run("rm -r /home/slave15/data_tmp", shell=True, capture_output=True)
    subprocess.run("mkdir /home/slave15/data_tmp", shell=True, capture_output=True)
    result = subprocess.run("touch /home/slave15/data_tmp/zcash.conf", shell=True, capture_output=True)
    property_file = "/home/slave15/data_tmp/zcash.conf"
    content = file_property.parse(property_file)
    content.put("listen", "1")
    content.put("server", "1")
    content.put("rpcuser", "root")
    content.put("rpcpassword", "root")
    content.put("server", "1")
    content.put("rpcport", "8232")
    content.put("rpcconnect", "127.0.0.1")
    content.put("prune", memory)
    while True:
        result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp &", shell=True)
        result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
        out=result.stdout.decode("utf-8")
        err=result.stderr.decode("utf-8")
        if "version" in out:
            break

def get_current_height(rpc_url="127.0.0.1:8232", rpc_user="root", rpc_password="root"):
    headers = {'content-type': 'application/json'}
    payload = []
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getinfo", "params": []})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        try:
            result = response.json()
            block_height = result[0]['result']['blocks']
            return block_height
        except ValueError as e:
            print("{'error': 'Invalid JSON response from server'}")
            get_current_height(rpc_url, rpc_user, rpc_password)
    else:
        print("error: " + f"HTTP error {response.status_code}")
        get_current_height(rpc_url, rpc_user, rpc_password)


def produce_params(current_step):
    params_list = []
    for i in range(30000):
        param_block_height = str(current_step * 30000 + i)
        param_list = [param_block_height,2]
        params_list.append(param_list)
    return params_list


def zcash_rpc(params_list):
    headers = {'content-type': 'application/json'}
    payload = []
    for params in params_list:
        payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getblock", "params": params})
    data = json.dumps(payload)
    url = f"http://root:root@127.0.0.1:8232"
    print(params_list)
    while True:
        response = requests.post(url, headers=headers, data=data)
        result = response.json()
        if result[-1]['result']:
            print(result[-1]['result']['height'])
            break
        else:
            continue

def check_lastblock(param):
    headers = {'content-type': 'application/json'}
    payload = []
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getblock", "params": param})
    data = json.dumps(payload)
    url = f"http://root:root@127.0.0.1:8232"
    print("check" + str(param))
    response = requests.post(url, headers=headers, data=data)
    result = response.json()
    try:
        print(result[0]['result']['height'])
        print("------normal status------")
        return 1
    except:
        print("------bad status------")
        return 0

def operate_zcash(status):
    if status == 1:
        while True:
            result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
            err=result.stderr.decode("utf-8")
            if "connect to server: unknown" in err:
                break
        while True:
            result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp &", shell=True)
            result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
            out=result.stdout.decode("utf-8")
            err=result.stderr.decode("utf-8")
            if "version" in out:
                break
    if status == 0:
        while True:
            result = subprocess.run("/home/slave15/zcash/src/zcash-cli stop", shell=True, capture_output=True)
            err=result.stderr.decode("utf-8")
            if "connect to server: unknown" in err:
                break
        while True:
            result = subprocess.run("nohup /home/slave15/zcash/src/zcashd -datadir=/home/slave15/data_tmp -connect=0 &", shell=True)
            result = subprocess.run("/home/slave15/zcash/src/zcash-cli getinfo", shell=True, capture_output=True)
            out=result.stdout.decode("utf-8")
            err=result.stderr.decode("utf-8")
            if "version" in out:
                break


if __name__ == "__main__":
    search_period = 0
    start_time = time.time()
    init()
    for i in range(int(300000/30000)):
        while True:
            current_block = get_current_height()
            if current_block >= (i+1)*30000:
                # change status
                if i != 0 :
                    tmp_param = [str((i-1)*30000),2]
                    status = check_lastblock(tmp_param)
                    operate_zcash(status)
                # query
                start_search_time = time.time()
                params_list = produce_params(i)
                zcash_rpc(params_list)
                end_search_time = time.time()
                search_period += end_search_time - start_search_time
                break
            else:
                time.sleep(((i+1)*30000 - current_block)/100)
    end_time = time.time()
    print("programme period: " + str(end_time - start_time))
    print("query period: " + str(search_period))
