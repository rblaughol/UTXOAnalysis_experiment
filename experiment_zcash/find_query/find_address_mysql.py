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
address_value = 't1NGuQeRWKSBEqz9B35HoUiYEwpzxJc7w7_'

query = f'''
    SELECT DISTINCT final_transaction 
    FROM (
        SELECT j3.transaction AS final_transaction FROM janusgraph j3 WHERE address = '{address_value}'
        
        UNION
        
        SELECT j2.transaction AS final_transaction
            FROM janusgraph j1, janusgraph j2
            WHERE j1.address = '{address_value}'
                AND j1.transaction = j2.previous_transaction
                AND j1.output_index = j2.previous_index
      ) as combined_txs;
'''

cursor.execute(query)
result = cursor.fetchall()
t2 = time.time()
"""



"""
"number 2"
t1 = time.time()
address_value = 't1NGuQeRWKSBEqz9B35HoUiYEwpzxJc7w7p_'
#query = f"SELECT transaction, output_index FROM janusgraph WHERE address = '{address_value}'"
query = f'''
    SELECT DISTINCT final_address
    FROM (
        SELECT j5.address as final_address 
            FROM janusgraph j4, janusgraph j5, janusgraph j3
            WHERE j3.address = '{address_value}'
                AND j3.transaction = j4.previous_transaction
                AND j3.output_index = j4.previous_index
                AND j4.transaction = j5.transaction

        UNION

        SELECT j1.address as final_address
            FROM janusgraph j1, janusgraph j2, janusgraph j3
            WHERE j3.address = '{address_value}'
                AND j3.transaction = j2.transaction
                AND j1.transaction = j2.previous_transaction
                AND j1.output_index = j2.previous_index
    ) as combined_addresses;
'''

cursor.execute(query)
#t2 = time.time()
result = cursor.fetchall()
t2 = time.time()
"""




"number 3"
t1 = time.time()
address_value = 't1NGuQeRWKSBEqz9B35HoUiYEwpzxJc7w7p_'
#query = f"SELECT transaction, output_index FROM janusgraph WHERE address = '{address_value}'"
query = f'''
    SELECT final_address 
    FROM (
        SELECT j5.address as final_address
            FROM janusgraph j4, janusgraph j5, janusgraph j3
            WHERE j3.address = '{address_value}'
                AND j3.transaction = j4.previous_transaction
                AND j3.output_index = j4.previous_index
                AND j4.transaction = j5.transaction

        UNION

        SELECT j1.address as final_address
            FROM janusgraph j1, janusgraph j2, janusgraph j3
            WHERE j3.address = '{address_value}'
                AND j3.transaction = j2.transaction
                AND j1.transaction = j2.previous_transaction
                AND j1.output_index = j2.previous_index

        UNION

        SELECT j8.address as final_address
            FROM janusgraph j5, janusgraph j6, janusgraph j7, janusgraph j8
            WHERE j5.address = '{address_value}'
                AND j5.transaction = j6.previous_transaction
                AND j5.output_index = j6.previous_index
                AND j6.transaction = j7.previous_transaction
                AND j7.transaction = j8.transaction

        UNION

        SELECT j8.address as final_address
            FROM janusgraph j5, janusgraph j6, janusgraph j7, janusgraph j8
            WHERE j5.address = '{address_value}'
                AND j5.transaction = j6.transaction
                AND j7.transaction = j6.previous_transaction
                AND j7.previous_transaction = j8.transaction
        

      ) as combined_addresses;
'''


cursor.execute(query)
result = cursor.fetchall()
t2 = time.time()


for row in result:
    print(row)
print(t2 - t1)
cursor.close()
conn.close()

