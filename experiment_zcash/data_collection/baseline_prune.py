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
def zcash_rpc(param):
    headers = {'content-type': 'application/json'}
    payload = []
    payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getblock", "params": param})
    data = json.dumps(payload)
    url = f"http://root:root@127.0.0.1:8232"
    print(param)
    while True:
        response = requests.post(url, headers=headers, data=data)
        result = response.json()
        if result[0]['result']:
            print(result[0]['result']['height'])
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
    print(time.time())
    init()
    for i in range(300000):
        param = [str(i),2]
        zcash_rpc(param)
        if i != 0 and i % 30000 == 0:
            param = [str(i-30000),2]
            status = check_lastblock(param)
            operate_zcash(status)
    print(time.time())
