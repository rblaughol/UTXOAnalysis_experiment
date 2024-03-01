import hashlib
import binascii
import base58
import os
import time
import file_property
import requests
import json

graphson_dir = "/public/home/blockchain/master/experiment_bitcoin/hyper/bitcoin_graphson/"
property_dir = "/public/home/blockchain/master/experiment_bitcoin/hyper/"

def batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method, params_list, windows_memory):
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
            if method == 'getblockhash':
                print('blockhash')
                return result
            else:
                property_file = property_dir + "bitcoin_data.properties"
                content = file_property.parse(property_file)
                block_num = int(content.get("bitcoin_block_height"))
                if block_num == result[0]['result']['height']:
                    pass
                if windows_memory == 10000 % 9000:
                    file_num = 1
                else:
                    #file_num = int(block_num/windows_memory)
                    file_num = int(block_num/windows_memory)
                block_num += windows_memory
                content.put("bitcoin_block_height", str(block_num))
                return result, file_num
        except:
            continue

def scriptPubKey_to_address(hex_data):
    hex_data = hex_data[2: -2]
    ripemd160 = hashlib.new('ripemd160')
    ripemd160.update(hashlib.sha256(binascii.unhexlify(hex_data)).digest())
    ripemd160hash = ripemd160.hexdigest ()
    d = int('00' + ripemd160hash+ hashlib.sha256(hashlib.sha256(binascii.unhexlify('00' + ripemd160hash)).digest()).hexdigest()[0:8],16)
    return '1' + str(base58.b58encode(d.to_bytes((d.bit_length() + 7) // 8, 'big')))[2:-1]

def calculate_file_hash(file_path):
    hash_object = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b''):
            hash_object.update(chunk)
    return hash_object.hexdigest()

def bitcoin_to_graphson(params_list,windows_memory):
    rpc_url = "127.0.0.1:8332"
    rpc_user = "root"
    rpc_password = "root"
    method1 = "getblockhash"
    method2 = "getblock"
    bithash_result = batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method1, params_list, windows_memory)
    bithash_list = []
    for hash_result in bithash_result:
        bithash = [str(hash_result['result']), 2]
        bithash_list.append(bithash)
    blocks, file_num= batch_bitcoin_rpc(rpc_url, rpc_user, rpc_password, method2, bithash_list, windows_memory)
    file_tx_dict = {}
    for json_block in blocks:
        block = json_block['result']
        # print(block)
        block_height = str(block['height'])
        timestamp = str(block['time'])

        txs = block['tx']
        for tx in txs:
            tx_id = tx['txid']
            transaction_dict = {}
            transaction_dict.update(id=tx_id)
            label = "transaction"
            transaction_dict.update(label=label)

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

            inE_dict.update(flow=inE_list)
            if len(tx['vin']) > 0:
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
            outE_dict.update(flow=outE_list)
            if len(tx['vout']) > 0:
                transaction_dict.update(outE=outE_dict)

            property_dict = {}
            transaction_size = str(tx["size"])
            # print(transaction_size)
            tmp_dict = {"id": "test_version", "value":transaction_size}
            property_dict.update(transaction_size=[tmp_dict])
            locktime = str(tx["locktime"])
            tmp_dict = {"id": "test_version", "value":locktime}
            property_dict.update(locktime=[tmp_dict])
            num_inputs = str(len(tx['vin']))
            tmp_dict = {"id": "test_version", "value":num_inputs}
            property_dict.update(num_inputs=[tmp_dict])
            num_outputs = str(len(tx['vout']))
            tmp_dict = {"id": "test_version", "value":num_outputs}
            property_dict.update(num_outputs=[tmp_dict])
            is_witness = "False"
            for vertex in (tx['vin']):
                if "txinwitness" in vertex:
                    is_witness = "True"
                    break
            tmp_dict = {"id": "test_version", "value":is_witness}
            property_dict.update(is_witness=[tmp_dict])
            tmp_dict = {"id":"test_version","value":block_height}
            property_dict.update(block_height=[tmp_dict])
            tmp_dict = {"id":"test_version","value":timestamp}
            property_dict.update(timestamp=[tmp_dict])
            transaction_dict.update(properties=property_dict)
            file_tx_dict[tx_id] = str(transaction_dict)


            for vin_index, vin in enumerate(tx['vin']):
                input_dict = {}
                vin_num = vin_index
                if len(str(vin_num)) < 3:
                    vin_num = str(vin_num).rjust(3, "0")
                else:
                    vin_num = str(vin_num)
                input_id = tx_id + vin_num + "in"
                input_dict.update(id=input_id)

                label = "UTXO"
                input_dict.update(label=label)


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
                            previous_index = previous_index.rjust(3,"0")
                        else:
                            previous_index = previous_index
                        outV_id = previous_transaction + previous_index + "out"
                        tmp_dict = {"id": "test_version", "outV": outV_id}
                        inE_dict.update(flow=[tmp_dict])
                        input_dict.update(inE=inE_dict)
                        tmp_out_dict = {}
                        tmp_out_dict.update(id=outV_id)
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

                        tmp_dict = {"id": "test_version", "inV": input_id}
                        tmp_out_dict2 = {}
                        tmp_out_dict2.update(flow=[tmp_dict])
                        values = eval(file_tx_dict[outV_id])
                        values.update(outE=tmp_out_dict2)
                        file_tx_dict[outV_id] = str(values)

                tmp_dict = {"id": "test_version", "inV": tx_id}
                outE_dict = {}
                outE_dict.update(flow=[tmp_dict])
                input_dict.update(outE=outE_dict)


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
                try:
                    witness_data = vin['txinwitness']
                    witness = "Witness("
                    for item in witness_data:
                        witness += item + " "
                    witness = witness.rstrip() + ")"
                except:
                    witness = ""
                tmp_dict = {"id": "test_version", "value": witness}
                property_dict.update(witness=[tmp_dict])
                input_dict.update(properties=property_dict)
                file_tx_dict[input_id] = str(input_dict)




            for vout_index, vout in enumerate(tx['vout']):
                output_dict = {}

                vout_num = str(vout['n'])
                if len(str(vout_num)) < 3:
                    vout_num = str(vout_num).rjust(3, "0")
                else:
                    vout_num = str(vout_num)
                output_id = tx_id + vout_num + "out"
                output_dict.update(id=output_id)

                label = "UTXO"
                output_dict.update(label=label)

                inE_dict = {}
                tmp_dict = {"id": "test_version", "outV": tx_id}
                inE_dict.update(flow=[tmp_dict])
                output_dict.update(inE=inE_dict)

                property_dict = {}

                value = str(vout['value'])
                tmp_dict = {"id": "test_version", "value": value}
                property_dict.update(value=[tmp_dict])

                index = str(vout['n'])
                tmp_dict = {"id": "test_version", "value": index}
                property_dict.update(index=[tmp_dict])

                pub_asm = vout["scriptPubKey"]["asm"]
                tmp_dict = {"id":"test_version", "value":pub_asm}
                property_dict.update(pub_asm=[tmp_dict])

                pub_hex = vout["scriptPubKey"]["hex"]
                tmp_dict = {"id":"test_version", "value":pub_hex}
                property_dict.update(pub_hex=[tmp_dict])

                try:
                    reqSigs = str(vout["scriptPubKey"]["reqSigs"])
                except:
                    reqSigs = ""
                tmp_dict = {"id":"test_version", "value":reqSigs}
                property_dict.update(reqSigs=[tmp_dict])

                address_type = vout["scriptPubKey"]["type"]
                tmp_dict = {"id":"test_version", "value": address_type}
                property_dict.update(address_type=[tmp_dict])
                address = ""
                if address_type == "nulldata":
                    address = ""
                elif address_type == "pubkey":
                    address = scriptPubKey_to_address(pub_hex) + '_'
                elif address_type == "nonstandard":
                    address = ""
                else:
                    for each_address in vout['scriptPubKey']['addresses']:
                        address += each_address + '_'

                tmp_dict = {"id": "test_version", "value": address}
                property_dict.update(address=[tmp_dict])

                output_dict.update(properties=property_dict)
                file_tx_dict[output_id] = str(output_dict)
    
    tx_string = "\n".join(list(file_tx_dict.values()))
    tx_string = tx_string.replace("'", "\"")
    
    if file_num >= 1:
        form_num = file_num - 1
        while True:
            try:
                form_json_file = "block" + str(form_num).rjust(5, "0") + ".json"
                hashlib1 = calculate_file_hash(graphson_dir + form_json_file)
                hashlib2 = calculate_file_hash("/public/home/blockchain/master/experiment_bitcoin/hyper/tmp/" + form_json_file)
                if hashlib1 == hashlib2:
                    break
            except:
                pass 
    current_json_file = "block" + str(file_num).rjust(5, "0") + ".json"
    with open(graphson_dir + current_json_file,'w') as fd:
        with open("/public/home/blockchain/master/experiment_bitcoin/hyper/tmp/" + current_json_file,'w') as fm:
            fm.write(tx_string)
            fm.close()
            while True:
                try:
                    property_file = "/public/home/blockchain/master/experiment_bitcoin/hyper/bitcoin_loader.properties"
                    content = file_property.parse(property_file)
                    current_json_transformed = int(content.get("current_json_transform")) + 1
                    content.put("current_json_transform", str(current_json_transformed))
                    break
                except:
                    continue
            fd.write(tx_string)
            fd.close()
