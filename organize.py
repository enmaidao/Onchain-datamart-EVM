""" 
Author: 
"""

def order_table_block(block, web3):
    """ build a block table to be compatible with SQLite data types"""
    block_data = web3.eth.getBlock(block)
    block_table = dict(block_data)
    
    #mapping keys to avoid name clashes
    m = {'hash':'blockHash', 'gasUsed':'blockGasUsed',
         'number':'blockNumber','logsBloom':'blockLogsBloom',
         'nonce':'blockNonce'}
    block_table = dict((m.get(k, k), v) for (k, v) in block_table.items())
    
    #convert types to be SQLite-compatible
    tostring = ['transactions', 'difficulty', 'totalDifficulty', 'uncles']
    tohex = ['blockHash', 'blockLogsBloom', 'blockNonce', 'extraData', 'mixHash', 'parentHash', 'receiptsRoot', 'sha3Uncles', 'stateRoot', 'transactionsRoot']
        
    for nn in block_table.keys():
        if nn in tohex:
            block_table[nn] = web3.toHex(block_table[nn]) 
        elif nn in tostring:
            block_table[nn] = str(block_table[nn]) 
    
    return block_table, block_data

def order_table_quick(hashh, block, web3, balance=False):
    """ build a Quick table to be compatible with SQLite data types; balance: do not read state; useful when the node still does full sync """
    #open transaction data
    tx_data = web3.eth.getTransaction(hashh)
    
    #get addresses
    addr_from = tx_data['from']
    addr_to = tx_data['to']

    #get balances of these addresses
    if balance:
        balance_from = web3.eth.getBalance(addr_from, block_identifier=block)
        try:
            balance_to = web3.eth.getBalance(addr_to, block_identifier=block)
        except TypeError:
            balance_to = -1
    else:
        balance_to = None
        balance_from = None
    #build a quick table
    quick_table = {}
    quick_keys = ['from', 'to', 'value', 'hash',
                  'nonce', 'blockNumber']
    
    #convert types to be SQLite-compatible
    for nn in quick_keys:
        if nn=="hash":
            quick_table["txHash"] = web3.toHex(tx_data[nn])
        elif nn=="value":
            quick_table["value"] = str(tx_data[nn])
        else:
            quick_table[nn] = tx_data[nn]
    #add balances
    quick_table['balanceTo'] = str(balance_to)
    quick_table['balanceFrom'] = str(balance_from)

    return quick_table, tx_data
    
def order_table_tx(tx_data, hashh, web3):
    """ build a TX table to be compatible with SQLite data types"""

    TX_table = dict(tx_data)
    # pop data already in Quick

    print("トランザクションあり")

    pop_tx_keys = ['from', 'to', 'value',
               'nonce', 'blockHash', 'hash']
    for nn in pop_tx_keys:
        TX_table.pop(nn)

    #add data from the receipt
    receipt_data = web3.eth.getTransactionReceipt(hashh)
    receipt_keys = ['contractAddress','cumulativeGasUsed',
              'gasUsed', 'gasUsed', 'logs', 'logsBloom',
               'status', 'transactionHash', 'transactionIndex']

    for nn in receipt_keys:
        try:
            if nn=="logs":
                TX_table[nn] = str(receipt_data[nn])
            elif nn=="logsBloom":
                TX_table[nn] = web3.toHex(receipt_data[nn])
            elif nn=='transactionHash':
                TX_table['txHash'] = receipt_data[nn]
            else:
                TX_table[nn] = receipt_data[nn]
        except KeyError:
            TX_table[nn] = -1
            
    tohex = ['r', 's', 'txHash']
    
    #conversion to strings
    for nn in TX_table.keys():
        if nn in tohex:
            TX_table[nn] = web3.toHex(TX_table[nn])        
    return TX_table

def order_table_contract(tx_data, block_data, quick_table, hashh, web3):

    """ build a TX table to be compatible with SQLite data types"""
    import requests
    import time
    import json
    import pandas as pd

    TX_table = dict(tx_data)
    Block_table = dict(block_data)['timestamp']
    quick_from = dict(quick_table)['to']
    contract = TX_table['contractAddress']

    if contract != None:

        print("contract生成トランザクション")

        # pop data
        pop_tx_keys = ['transactionIndex', 'raw', 'publicKey', 'chainId', 'standardV', 'v', 'r', 's', 'accessList', 'logsBloom', 'creates']
        for nn in pop_tx_keys:
            TX_table.pop(nn)
        
        df_token_txns = order_table_token(contract, web3)

        if any(df_token_txns):
            token_table = df_token_txns.to_dict('records')[0]
        else:
            token_table = dict()

        ##############

        #add abi data from the blockscout
        abi_keys = ['verify','ABI','CompilerVersion','ContractName','FileName','IsProxy','OptimizationUsed']
        abi_url = f'https://blockscout.com/astar/api?module=contract&action=getsourcecode&address={contract}'
        abi_data = []

        con_table = dict()

        while True:
            _abi_url_data = requests.get(abi_url)
            time.sleep(1)

            if _abi_url_data.ok:
                abi_url_data = dict(json.loads(_abi_url_data.text))
                break

        if abi_url_data["status"] == "0":
                print("verifyされてないcontract")

        else:
            for nn in abi_keys:
                try:
                    if nn=="verify":
                        con_table['verify'] = abi_url_data['status']
                    else:
                        con_table[nn] = str(abi_url_data['result'][0][nn])
                except KeyError:
                    con_table[nn] = -1
        
        con_table.update(TX_table)
        con_table['timestamp'] = Block_table
        con_table['to'] = quick_from

        

        ######################decode#######################
        # myContract = web3.eth.contract(address=contract, abi=str(con_table['ABI']))
        # transaction = web3.eth.get_transaction(hashh)
        # func_obj, func_params =myContract.decode_function_input(transaction.input)
        # print(func_obj)
        # con_table['input_decoded'] = myContract.decode_function_input(transaction.input)

        # print(con_table['input_decoded'])

        #####################################################
    
    else:
        con_table = dict()
        token_table = dict()
    
    return con_table, token_table


def order_table_token(contract, web3):

    import requests
    import time
    import json
    import pandas as pd

    token_url = []

    token_url.append(
        f'https://blockscout.com/astar/api?module=token&action=getToken&contractaddress={contract}')

    token_response_data = []
    for i, v in enumerate(token_url):
        while True:
            data = requests.get(v)
            time.sleep(1)
            if data.ok:
                token_response_data.append(json.loads(data.text)) 
                break

    df_token_txns = pd.DataFrame()

    for i, data_sample in enumerate(token_response_data):
        if data_sample["status"] == "0":
            print('tokenコントラクトではない')
            pass      
        else:
            print('tokenコントラクト')
            token_b_df = pd.DataFrame(data_sample['result'],index=['{}'.format(i),])

            if token_b_df["decimals"][0] != '':
                #レコードからNFT除く
                token_df = token_b_df[token_b_df["decimals"] != '']
                
                decimals = token_b_df['decimals'][0]

                token_df = token_df.loc[:, ['cataloged', 'contractAddress', 'decimals', 'name','symbol',"totalSupply","type"]]
                # token_df['totalSupply'] = token_df['totalSupply'].astype('float128')/pow(10, token_b_df['decimals'].astype('float128'))
                
                df_token_txns = pd.concat([df_token_txns, token_df])
                
            else:
                #レコードからNFT抽出
                nft_df = token_b_df[token_b_df["decimals"] == '']

                decimals = token_b_df['decimals'][0]

                if any(nft_df["totalSupply"]) == False:
                    nft_df["totalSupply"] = -1
                    # print(nft_df["totalSupply"])
                else:
                    pass

                nft_df = nft_df.loc[:, ['cataloged', 'contractAddress', 'decimals', 'name','symbol',"totalSupply","type"]]
                
                df_token_txns = pd.concat([df_token_txns, nft_df])

                # print(df_token_txns)
                print('This token is NFT')

    return df_token_txns

def token_holders(decimals, contract):
    import requests
    import json
    import pandas as pd
    import datetime
    import time

    #token holdersに関する統計情報    
    token_holders_url = []

    token_holders_url.append(
        f'https://blockscout.com/astar/api?module=token&action=getTokenHolders&contractaddress={contract}')
    
    token_holders_response_data = []

    for i, v in enumerate(token_holders_url):
        while True:
            data = requests.get(v)
            time.sleep(1)
            if data.ok:
                token_holders_response_data.append(json.loads(data.text)) 
                break

    df_token_holders_txns = pd.DataFrame()

    for i, data_holders_sample in enumerate(token_holders_response_data):
        if data_holders_sample["status"] == "0": 
            print("tokenではないcontract")

        elif decimals == '':
            token_holders_df = pd.DataFrame(data_holders_sample['result'])
            df_token_holders_txns = pd.concat([df_token_holders_txns, token_holders_df])

        else:
            token_holders_df = pd.DataFrame(data_holders_sample['result'])
            token_holders_df['desimals'] = decimals
            token_holders_df['value'] = token_holders_df['value'].astype('float128')/pow(10, token_holders_df['desimals'].astype('float128'))
            
            df_token_holders_txns = pd.concat([df_token_holders_txns, token_holders_df])
    
    return df_token_holders_txns


def execute_sql(table_quick, table_tx, table_block, table_token, table_contract):
    import os
    from sql_helper import create_database, update_database, create_index
    import psycopg2 as ps

    # define credentials 
    credentials = {'POSTGRES_ADDRESS' : '', # change to your endpoint
                'POSTGRES_PORT' : '', # change to your port
                'POSTGRES_USERNAME' : '', # change to your username
                'POSTGRES_PASSWORD' : '', # change to your password
                'POSTGRES_DBNAME' : ''} # change to your db name

    # create connection and cursor    
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                    database=credentials['POSTGRES_DBNAME'],
                    user=credentials['POSTGRES_USERNAME'],
                    password=credentials['POSTGRES_PASSWORD'],
                    port=credentials['POSTGRES_PORT'])
    cur = conn.cursor()

    db_is_new = "select tablename from pg_tables;"
    cur.execute(db_is_new)
    d = cur.fetchall()
    
    if len(d)<=66:
        print('Creating a new DB.')
        create_database(cur)
        create_index(cur)
        update_database(cur,table_quick, table_tx, table_block, table_token, table_contract)
    else:
        update_database(cur ,table_quick, table_tx, table_block, table_token, table_contract)
    conn.commit()
    conn.close()

