import mysql.connector
import time

config = {
    'user': 'janusgraph',
    'password': 'janusgraph',
    'host': 'localhost',
    'database': 'janusgraph',
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()


"""
"number 1"
t1 = time.time()
transaction_value = 'c8b37d74c40ca83d5078966f5b7919ac7b0a06e572c0acc66256ecdff2760814'

query = f'''
    SELECT DISTINCT final_address
    FROM (
        SELECT j3.address AS final_address FROM janusgraph j3 WHERE transaction = '{transaction_value}'
        
        UNION
        
        SELECT j2.address AS final_address
            FROM janusgraph j1, janusgraph j2
            WHERE j1.transaction = '{transaction_value}'
                AND j2.transaction = j1.previous_transaction
      ) as combined_addresses;
'''

cursor.execute(query)
result = cursor.fetchall()
t2 = time.time()
"""


"""
"number 2"
t1 = time.time()
transaction_value = 'c8b37d74c40ca83d5078966f5b7919ac7b0a06e572c0acc66256ecdff2760814'
#query = f"SELECT transaction, output_index FROM janusgraph WHERE address = '{address_value}'"
query = f'''
    SELECT DISTINCT final_tx
    FROM (
        SELECT j4.transaction as final_tx
            FROM janusgraph j4, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.transaction = j4.previous_transaction
                

        UNION

        SELECT j2.transaction as final_tx
            FROM janusgraph j2, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.previous_transaction = j2.transaction
    ) as combined_txs;
'''

cursor.execute(query)
#t2 = time.time()
result = cursor.fetchall()
t2 = time.time()
"""




"number 3"
t1 = time.time()
transaction_value = 'c8b37d74c40ca83d5078966f5b7919ac7b0a06e572c0acc66256ecdff2760814'
#query = f"SELECT transaction, output_index FROM janusgraph WHERE address = '{address_value}'"
query = f'''
    SELECT DISTINCT final_tx
    FROM (
        SELECT j4.transaction as final_tx
            FROM janusgraph j4, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.transaction = j4.previous_transaction


        UNION

        SELECT j2.transaction as final_tx
            FROM janusgraph j2, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.previous_transaction = j2.transaction

        UNION

        SELECT j5.transaction as final_tx
            FROM janusgraph j5, janusgraph j4, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.transaction = j4.previous_transaction
                AND j4.transaction = j5.previous_transaction

        UNION

        SELECT j1.transaction as final_tx
            FROM janusgraph j1, janusgraph j2, janusgraph j3
            WHERE j3.transaction = '{transaction_value}'
                AND j3.previous_transaction = j2.transaction
                AND j2.previous_transaction = j1.transaction

) as combined_txs;
'''



cursor.execute(query)
#t2 = time.time()
result = cursor.fetchall()
t2 = time.time()




for row in result:
    print(row)
print(t2 - t1)
cursor.close()
conn.close()

