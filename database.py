""" 
Author: enmai
"""

from web3 import Web3
from organize import order_table_block,order_table_quick,order_table_tx,execute_sql,order_table_contract
import time
import requests

astar_url = "https://astar.blastapi.io/"
web3 = Web3(Web3.HTTPProvider(astar_url))
print(web3.isConnected())

# load a block.
Nblocks = 200000
output_every = 2
start_time = time.time()
try:
    with open('lastblock.txt', 'r') as f:
        start = int(f.read())+1
except FileNotFoundError:
    start = 200000

#define tables that will go to the MySQL database
table_quick = []
table_tx = []
table_block = []
table_contract = []
table_token = []

count = 0
#loop over all blocks
for block in range(start, start+Nblocks):
    block_table, block_data = order_table_block(block,web3)
    #list of block data that will go to the DB
    table_block.append(block_table)
    #all transactions on the block
    print("==========================================")

    print(block_data)
    
    for hashh in block_data['transactions']:
        #print(web3.toHex(hashh))       
        quick_table, tx_data = order_table_quick(hashh, block, web3)
        table_quick.append(quick_table)
        
        #list of tx data that will go to the DB
        TX_table = order_table_tx(tx_data, hashh, web3)
        table_tx.append(TX_table)
        
        #list of contract data that will go to the DB
        con_table, token_table = order_table_contract(TX_table, block_data, quick_table, hashh, web3)
        table_contract.append(con_table)
        table_token.append(token_table)

    count = count + 1
    #print(count)
    #dump output every 2 blocks
    if (count % output_every) == 0:
        execute_sql(table_quick, table_tx, table_block, table_token, table_contract)
        
        #free up memory
        del table_quick
        del table_tx
        del table_block
        del table_token
        del table_contract
        table_quick = []
        table_tx = []
        table_block = []
        table_token = []
        table_contract = []
        
        #update the current block number to a file
        with open('lastblock.txt', 'w') as f:
            f.write("%d" % block)
    if (count % 10) == 0:
        end = time.time()
        with open('timeperXblocks.txt', 'a') as f:
            f.write("%d %f \n" % (block, end-start_time))
    if (count % 100) == 0:
        print("100 new blocks completed.")