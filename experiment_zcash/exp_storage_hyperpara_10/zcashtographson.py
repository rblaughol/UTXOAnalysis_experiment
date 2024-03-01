import time
import os
import paramiko
import hashlib
from scp import SCPClient
import file_property
import requests
import json

graphson_dir = "/home/slave15/zcash_graphson/"

def batch_zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list,windows_memory):
    headers = {'content-type': 'application/json'}
    payload = []
    for params in params_list:
        payload.append({"jsonrpc": "1.0", "id": "curltest", "method": method, "params": params})
    data = json.dumps(payload)
    url = f"http://{rpc_user}:{rpc_password}@{rpc_url}"
    while True:
        try:
            response = requests.post(url, headers=headers, data=data)
            #print(response)
            #print(result[0]['result']['height'])
            result = response.json()
            #print(result)
            #print(result[0]['result']['height'])
            property_file = "/home/slave15/experiment/exp_storage_hyperpara_10/zcash_data.properties"
            #print(123)
            content = file_property.parse(property_file)
            #print(456)
            if result[0]['result']['height']:
                pass
            #print(456, result[0]['result']['height'])
            block_num = int(content.get("zcash_block_height"))
            file_num = int(block_num/windows_memory)
            block_num += windows_memory
            content.put("zcash_block_height", str(block_num))
            return result, file_num
        except:
            continue
def calculate_file_hash(file_path):
    hash_object = hashlib.md5()

    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            hash_object.update(chunk)

    return hash_object.hexdigest()

def zcash_to_graphson(params_list,windows_memory):
    #time.sleep(5)
    rpc_url = "127.0.0.1:8232"
    rpc_user = "root"
    rpc_password = "root"
    method = "getblock"
    blocks, file_num = batch_zcash_rpc(rpc_url, rpc_user, rpc_password, method, params_list,windows_memory)
    # start to transform
    file_tx_dict = {}
    #print(raw_data)
    #print(len(blocks))
    #time.sleep(50)
    for json_block in blocks:
        block = json_block['result']
        # the block information which is the same in txs
        block_height = str(block['height'])
        timestamp = str(block['time'])
        
        txs = block['tx']
        for tx in txs:
            # the tx informatiom
            tx_id = tx['txid']

            # the transaction dict whose order is id, label, E, properties
            transaction_dict = {}

            # main id in graphson
            transaction_dict.update(id=tx_id)
            
            # label in graphson
            label = "transaction"
            transaction_dict.update(label=label)
            
            # edge in graphson
            inE_dict = {}
            inE_list = []
            for vin_num in range(len(tx['vin'])):
                if vin_num < 100:
                    vin_num = str(vin_num).rjust(3, "0")
                else:
                    vin_num = str(vin_num)
                input_id = tx_id + vin_num + "in"
                tmp_dict = {"id": "test_version", "outV": input_id}
                inE_list.append(tmp_dict)
            """
            if len(tx['vin'])==0:
                tmp_dict = {"id": "test_version", "outV": "shielded"}
                inE_list.append(tmp_dict)
            """
            inE_dict.update(flow=inE_list)
            transaction_dict.update(inE=inE_dict)

            outE_dict = {}
            outE_list = []
            for vout_num in range(len(tx['vout'])):
                if len(str(vout_num)) < 3:
                    vout_num = str(vout_num).rjust(3, "0")
                else:
                    vout_num = str(vout_num)
                output_id = tx_id + vout_num + "out"
                tmp_dict = {"id": "test_version", "inV": output_id}
                outE_list.append(tmp_dict)
            """
            if len(tx['vout'])==0:
                tmp_dict = {"id": "test_version", "inV": "shielded"}
                outE_list.append(tmp_dict)
            """
            outE_dict.update(flow=outE_list)
            transaction_dict.update(outE=outE_dict)

            # the properties in graphson
            property_dict = {}
            tmp_dict = {"id":"test_version","value":block_height}
            property_dict.update(block_height=[tmp_dict])
            tmp_dict = {"id":"test_version","value":timestamp}
            property_dict.update(timestamp=[tmp_dict])
            transaction_dict.update(properties=property_dict)
            
            # put transaction graphson in the graphson file
            file_tx_dict[tx_id] = str(transaction_dict)



            # produce the input_node
            """
            if len(tx['vin']) == 0:
                input_dict = {}
                input_dict.update(id="shielded")
                label = "UTXO"
                input_dict.update(label=label)
                tmp_dict = {"id": "test_version", "inV": tx_id}
                outE_dict = {}
                outE_dict.update(flow=[tmp_dict])
                input_dict.update(outE=outE_dict)
                file_tx_dict[input_id] = str(input_dict)
            """
            for vin_index,vin in enumerate(tx['vin']):
                # the input_node dict whose order is id, label, E, properties
                input_dict = {}
                
                # main id in graphson 
                vin_num = vin_index
                if len(str(vin_num)) < 3:
                    vin_num = str(vin_num).rjust(3, "0")
                else:
                    vin_num = str(vin_num)
                input_id = tx_id + vin_num + "in"
                input_dict.update(id=input_id)
                
                # label in graphson
                label = "UTXO"
                input_dict.update(label=label)

                # edge in graphson 
                inE_dict = {}
                sequence = str(vin['sequence'])
                try:
                    coinbase = vin['coinbase']
                    previous_transaction = ""
                    previous_index = ""
                    sig_asm = ""
                    sig_hex = ""
                except:
                    coinbase = ""
                    previous_transaction = vin['txid']
                    previous_index = str(vin['vout'])
                    sig_asm = vin['scriptSig']['asm']
                    sig_hex = vin['scriptSig']['hex']
                if coinbase == "":
                    if previous_transaction not in file_tx_dict:
                        if len(previous_index) < 3:
                            previous_index = previous_index.rjust(3, "0")
                        else:
                            previous_index = previous_index
                        outV_id = previous_transaction + previous_index + "out"
                        tmp_dict = {"id": "test_version", "outV": outV_id}
                        inE_dict.update(flow=[tmp_dict])
                        input_dict.update(inE=inE_dict)
                        tmp_out_dict = {}
                        # another
                        tmp_out_dict.update(id=outV_id)
                        # label
                        label = "UTXO"
                        tmp_out_dict.update(label=label)
                        tmp_dict = {"id": "test_version", "inV": input_id}
                        tmp_out_dict2 = {}
                        tmp_out_dict2.update(flow=[tmp_dict])
                        tmp_out_dict.update(outE=tmp_out_dict2)
                        file_tx_dict[outV_id] = str(tmp_out_dict)
                    else:
                        if len(previous_index) < 3:
                            previous_index = previous_index.rjust(3, "0")
                        else:
                            previous_index = previous_index
                        outV_id = previous_transaction + previous_index + "out"
                        tmp_dict = {"id": "test_version", "outV": outV_id}
                        inE_dict.update(flow=[tmp_dict])
                        input_dict.update(inE=inE_dict)
                        # write to file_dict
                        outV_id = previous_transaction + previous_index + "out"
                        tmp_dict = {"id": "test_version", "inV": input_id}
                        tmp_out_dict2 = {}
                        tmp_out_dict2.update(flow=[tmp_dict])
                        values = eval(file_tx_dict[outV_id])
                        values.update(outE=tmp_out_dict2)
                        file_tx_dict[outV_id] = str(values)
    
                
                # the content of the edge(to tx) of input_id
                tmp_dict = {"id": "test_version", "inV": tx_id}
                outE_dict = {}
                outE_dict.update(flow=[tmp_dict])
                input_dict.update(outE=outE_dict)
                
                # the properties in graphson
                property_dict = {}
                tmp_dict = {"id": "test_version", "value": coinbase}
                property_dict.update(coinbase=[tmp_dict])
                tmp_dict = {"id": "test_version", "value": sequence}
                property_dict.update(sequence=[tmp_dict])
                tmp_dict = {"id": "test_version", "value": previous_transaction}
                property_dict.update(previous_transaction=[tmp_dict])
                tmp_dict = {"id": "test_version", "value": previous_index}
                property_dict.update(previous_index=[tmp_dict])
                tmp_dict = {"id": "test_version", "value": sig_asm}
                property_dict.update(sig_asm=[tmp_dict])
                tmp_dict = {"id": "test_version", "value": sig_hex}
                property_dict.update(sig_hex=[tmp_dict])
                input_dict.update(properties=property_dict)            
                file_tx_dict[input_id] = str(input_dict)
            
                
            
            # produce the output_node
            """
            if len(tx['vout']) == 0:
                output_dict = {}
                output_dict.update(id="shielded")
                label = "UTXO"
                output_dict.update(label=label)
                tmp_dict = {"id": "test_version", "outV": tx_id}
                inE_dict = {}
                inE_dict.update(flow=[tmp_dict])
                output_dict.update(inE=inE_dict)
                file_tx_dict[output_id] = str(output_dict)
            """
            for vout_index,vout in enumerate(tx['vout']):
                # the output_node dict whose order is id, label, E, properties
                output_dict = {}

                # main id in graphson
                vout_num = str(vout['n'])
                if len(str(vout_num)) < 3:
                    vout_num = str(vout_num).rjust(3, "0")
                else:
                    vout_num = str(vout_num)
                output_id = tx_id + vout_num + "out"
                output_dict.update(id=output_id)

                # label in graphson
                label = "UTXO"
                output_dict.update(label=label)

                # edge in graphson
                inE_dict = {}
                tmp_dict = {"id": "test_version", "outV": tx_id}
                inE_dict.update(flow=[tmp_dict])
                output_dict.update(inE=inE_dict)
                
                # properties in graphson
                property_dict = {}
                value = str(vout['value'])
                tmp_dict = {"id":"test_version","value":value}
                property_dict.update(value=[tmp_dict])
                valueZat = str(vout['valueZat'])
                tmp_dict = {"id":"test_version","value":valueZat}
                property_dict.update(valueZat=[tmp_dict])
                valueSat = str(vout['valueSat'])
                tmp_dict = {"id":"test_version","value":valueSat}
                property_dict.update(valueSat=[tmp_dict])
                index = str(vout['n'])
                tmp_dict = {"id":"test_version","value":index}
                property_dict.update(index=[tmp_dict])
                address_type = vout['scriptPubKey']['type']
                tmp_dict = {"id":"test_version","value":address_type}
                property_dict.update(address_type=[tmp_dict])
                pub_asm = vout['scriptPubKey']['asm']
                tmp_dict = {"id":"test_version","value":pub_asm}
                property_dict.update(pub_asm=[tmp_dict])
                pub_hex = vout['scriptPubKey']['hex']
                tmp_dict = {"id":"test_version","value":pub_hex}
                property_dict.update(pub_hex=[tmp_dict])
                address = ""
                try:
                    for each_address in vout['scriptPubKey']['addresses']:
                        address += each_address + '_'
                    tmp_dict = {"id":"test_version","value":address}
                    property_dict.update(address=[tmp_dict])
                except:
                    tmp_dict = {"id":"test_version","value":'shielded'}
                    property_dict.update(address=[tmp_dict])
                output_dict.update(properties=property_dict)
                file_tx_dict[output_id] = str(output_dict)


    tx_string = "\n".join(list(file_tx_dict.values()))
    tx_string = tx_string.replace("'", "\"")
    # write to graphson
    if file_num >= 1:
        form_num = file_num - 1
        # check the hash to avoid that the process is writing
        while True:
            try:
                form_json_file = "block" + str(form_num).rjust(5, "0") + ".json"
                if os.path.getsize("/home/slave15/tmp/" + form_json_file) > 0:
                    hashlib1 = calculate_file_hash(graphson_dir + form_json_file)
                    hashlib2 = calculate_file_hash("/home/slave15/tmp/" + form_json_file)
                    #print(hashlib1,hashlib2)
                    if hashlib1 == hashlib2:
                        break
            except:
                pass
    #print("here"+current_json_file)
    current_json_file = "block" + str(file_num).rjust(5, "0") + ".json"
    #print(current_json_file)
    # tmp_json_file = "tmp" + str(file_num).rjust(5, "0") + ".json"

    with open(graphson_dir + current_json_file,'w') as fd:
        with open("/home/slave15/tmp/" + current_json_file,'w') as fm:
            fm.write(tx_string)
            fm.close()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect('192.168.31.1', username='master', password='Ppsuc@gu2024')
            scp = SCPClient(ssh.get_transport())
            local_file_path = '/home/slave15/tmp/' + current_json_file
            remote_file_path = '/public/home/blockchain/master/zcash_graphson/'
            while True:
                try:
                    scp.put(local_file_path, remote_file_path)
                    break
                except:
                    continue
            print(current_json_file)
            while True:
                try:
                    property_file = "/home/slave15/experiment/exp_storage_hyperpara_10/zcash_loader.properties"
                    content = file_property.parse(property_file)
                    current_json_transformed = int(content.get("current_json_transform")) + 1
                    content.put("current_json_transform", str(current_json_transformed))
                    break
                except:
                    continue
            print(current_json_transformed)
            while True:
                try:
                    remote_file_path = '/public/home/blockchain/master/experiment/exp_storage_hyperpara_10/'
                    scp.put(property_file, remote_file_path)
                    break
                except:
                    continue

            scp.close()
            ssh.close()
            # transmit the properties before writing the graphson
            fd.write(tx_string)
            fd.close()
            #time.sleep(150)

