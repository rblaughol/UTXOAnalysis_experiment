import mysql.connector
import time
import os
import paramiko
import hashlib
from scp import SCPClient
import file_property
import requests
import json


conn = mysql.connector.connect(
        host="localhost",
        user="janusgraph",
        password="janusgraph",
        database="janusgraph"
        )

cursor = conn.cursor()

table_create_query="""
    create TABLE janusgraph(
        id INT AUTO_INCREMENT PRIMARY KEY,
        transaction VARCHAR(200),
        block_length VARCHAR(50),
        timestamp VARCHAR(50),
        coinbase VARCHAR(50),
        previous_transaction VARCHAR(200),
        previous_index VARCHAR(50),
        weight VARCHAR(50),
        valueZat VARCHAR(50),
        valueSat VARCHAR(50),
        output_index VARCHAR(50),
        address_type VARCHAR(50),
        address VARCHAR(200)
        )
"""
cursor.execute(table_create_query)
conn.commit()

index_creation="""
    create index tx_index on janusgraph (transaction)
"""
cursor.execute(index_creation)

index_creation="""
    create index address_index on janusgraph (address)
"""
cursor.execute(index_creation)

index_creation="""
    create index address_type_index on janusgraph (address_type)
"""
cursor.execute(index_creation)

index_creation="""
    create index weight_index on janusgraph (weight)
"""
cursor.execute(index_creation)

index_creation="""
    create index blocklength_index on janusgraph (block_length)
"""
cursor.execute(index_creation)

index_creation="""
    create index timestamp_index on janusgraph (timestamp)
"""
cursor.execute(index_creation)

conn.commit()



for block_num in range(300):
    params_list = []
    for i in range(1000):
        param_block_height = str(i+1000*block_num)
        param_list = [param_block_height,2]
        params_list.append(param_list)

    headers = {'content-type': 'application/json'}
    payload = []
    for params in params_list:
        payload.append({"jsonrpc": "1.0", "id": "curltest", "method": "getblock", "params": params})
    data = json.dumps(payload)
    url = f"http://root:root@127.0.0.1:8232"
    while True:
        try:
            response = requests.post(url, headers=headers, data=data)
            result = response.json()
            #print(result)
            if result[-1]['result']['height']:
                break
        except:
            continue

    # parse
    for json_block in result:
        block = json_block['result']
        # the block information which is the same in txs
        block_height = str(block['height'])
        timestamp = str(block['time'])
        txs = block['tx']
        for tx in txs:
            # the tx informatiom
            tx_id = tx['txid']
            for vin_index,vin in enumerate(tx['vin']):
                try:
                    coinbase = vin['coinbase']
                    coinbase = "True"
                    previous_transaction = "NULL"
                    previous_index = "NULL"
                except:
                    coinbase = "NULL"
                    previous_transaction = vin['txid']
                    previous_index = str(vin['vout'])

                data_insert = f"""
                insert into janusgraph (transaction, block_length, timestamp, coinbase, previous_transaction, previous_index) values ('{tx_id}', '{block_height}', '{timestamp}', '{coinbase}', '{previous_transaction}', '{previous_index}')
            """
                #print(data_insert)
                cursor.execute(data_insert)
            
            for vout_index,vout in enumerate(tx['vout']):
                value = str(vout['value'])
                valueZat = str(vout['valueZat'])
                valueSat = str(vout['valueSat'])
                index = str(vout['n'])
                address_type = vout['scriptPubKey']['type']
                address = ""
                try:
                    for each_address in vout['scriptPubKey']['addresses']:
                        address += each_address + '_'
                except:
                    address = "shielded"
                #print(address)
                data_insert = f"""
                insert into janusgraph (transaction, weight, valueZat, valueSat, output_index, address_type, address) values ('{tx_id}', '{value}', '{valueZat}', '{valueSat}', '{index}', '{address_type}', '{address}')
            """
                #print(data_insert)
                cursor.execute(data_insert)
        print(block_height)
        conn.commit()
