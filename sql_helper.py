""" 
Author: 
"""
def create_database(cur):
    """ create the schema for the database"""
    quick = """
    CREATE TABLE IF NOT EXISTS Quick (
     balanceFrom TEXT,
     balanceTo TEXT,
     blockNumber INTEGER, 
     sender TEXT,
     nonce INTEGER, 
     recipient TEXT,
     txHash TEXT PRIMARY KEY,
     value TEXT);"""

    tx = """
    CREATE TABLE IF NOT EXISTS TX (
     blockNumber INTEGER,
     contractAddress TEXT,
     cumulativeGasUsed INTEGER, 
     gas INTEGER, 
     gasPrice bigint,
     gasUsed INTEGER,
     input TEXT, 
     logs TEXT, 
     logsBloom TEXT, 
     r TEXT, 
     s TEXT, 
     status INTEGER, 
     txHash TEXT PRIMARY KEY,
     transactionIndex INTEGER, 
     v INTEGER);"""

    blck = """
    CREATE TABLE IF NOT EXISTS block ( 
     author TEXT,
     blockGasUsed INTEGER, 
     blockHash TEXT, 
     blockLogsBloom TEXT, 
     blockNonce TEXT, 
     blockNumber INTEGER PRIMARY KEY, 
     difficulty TEXT, 
     extraData TEXT,  
     gasLimit INTEGER, 
     miner TEXT, 
     mixHash TEXT,      
     parentHash TEXT, 
     receiptsRoot TEXT, 
     sha3Uncles TEXT, 
     size INTEGER, 
     stateRoot TEXT, 
     timestamp INTEGER, 
     totalDifficulty TEXT, 
     transactions TEXT, 
     transactionsRoot TEXT, 
     uncles TEXT); """

    token = """
    CREATE TABLE IF NOT EXISTS token ( 
     cataloged TEXT,
     contractAddress TEXT PRIMARY KEY,
     decimals TEXT,
     name TEXT,
     symbol TEXT,
     totalSupply numeric,
     type TEXT); """

    contract = """
    CREATE TABLE IF NOT EXISTS contract ( 
     timestamp INTEGER,
     blockNumber INTEGER,
     contractAddress TEXT,
     ContractName TEXT,
     transactionTo TEXT,
     txHash TEXT PRIMARY KEY,
     verify TEXT, 
     status TEXT,
     cumulativeGasUsed INTEGER,
     gas INTEGER,
     gasPrice bigint,
     gasUsed INTEGER,
     CompilerVersion TEXT,
     FileName TEXT,
     IsProxy TEXT,
     OptimizationUsed TEXT,
     type TEXT,
     input TEXT,
     logs TEXT,
     ABI TEXT); """

    cur.execute(quick)
    cur.execute(blck)
    cur.execute(tx)
    cur.execute(token)
    cur.execute(contract)

def create_index(cur):
    quick = "CREATE INDEX index_quick ON Quick(value, sender, recipient);"
    tx = "CREATE INDEX index_TX ON TX(blockNumber, status);"
    blck = "CREATE INDEX index_block ON block(timestamp);"
    token = "CREATE INDEX index_token ON token(name);"
    contract = "CREATE INDEX index_contract ON contract(timestamp, blockNumber);"

    cur.execute(quick)
    cur.execute(blck)
    cur.execute(tx)
    cur.execute(token)
    cur.execute(contract)
    
def update_database(cur, table_quick, table_tx, table_block, table_token, table_contract):
    import sys

    """ write lists of dictionaries into the database"""
    quick = """INSERT INTO quick (balanceFrom, balanceTo, blockNumber, sender, nonce, recipient, txHash, value) VALUES (%(balanceFrom)s, %(balanceTo)s, %(blockNumber)s, %(from)s, %(nonce)s, %(to)s, %(txHash)s, %(value)s);"""
    tx = """INSERT INTO tx (blockNumber, contractAddress, cumulativeGasUsed, gas, gasPrice, gasUsed, input, logs, logsBloom, r, s, status, txHash, transactionIndex, v) VALUES (%(blockNumber)s, %(contractAddress)s, %(cumulativeGasUsed)s, %(gas)s, %(gasPrice)s, %(gasUsed)s, %(input)s, %(logs)s, %(logsBloom)s, %(r)s, %(s)s, %(status)s, %(txHash)s, %(transactionIndex)s, %(v)s);"""
    block = """INSERT INTO block (author, blockGasUsed, blockHash, blockLogsBloom, blockNonce, blockNumber, difficulty, extraData, gasLimit, miner, parentHash, receiptsRoot, sha3Uncles, size, stateRoot, timestamp, totalDifficulty, transactions, transactionsRoot, uncles) VALUES (%(author)s, %(blockGasUsed)s, %(blockHash)s, %(blockLogsBloom)s, %(blockNonce)s, %(blockNumber)s, %(difficulty)s, %(extraData)s, %(gasLimit)s, %(miner)s, %(parentHash)s, %(receiptsRoot)s, %(sha3Uncles)s, %(size)s, %(stateRoot)s, %(timestamp)s, %(totalDifficulty)s, %(transactions)s, %(transactionsRoot)s, %(uncles)s);"""
    token = """INSERT INTO token (cataloged, contractAddress, decimals, name, symbol, totalSupply, type) VALUES (%(cataloged)s, %(contractAddress)s, %(decimals)s, %(name)s, %(symbol)s, %(totalSupply)s, %(type)s);"""
    contract = """INSERT INTO contract (timestamp, blockNumber, contractAddress, ContractName, transactionTo, txHash, verify, status, cumulativeGasUsed, gas, gasPrice, gasUsed, CompilerVersion, FileName, IsProxy, OptimizationUsed, type, input, logs, ABI) VALUES (%(timestamp)s, %(blockNumber)s, %(contractAddress)s, %(ContractName)s, %(to)s, %(txHash)s, %(verify)s, %(status)s, %(cumulativeGasUsed)s, %(gas)s, %(gasPrice)s, %(gasUsed)s, %(CompilerVersion)s, %(FileName)s, %(IsProxy)s, %(OptimizationUsed)s, %(type)s, %(input)s, %(logs)s, %(ABI)s);"""

    cur.executemany(quick, table_quick)
    cur.executemany(tx, table_tx)
    cur.executemany(block, table_block)
    
    if any(table_token) == True:
        table_token = list(filter(None, table_token))
        print(table_token)
        if table_token != None:
            cur.executemany(token, table_token)
        else:
            pass
    else:
        pass

    if any(table_contract) == True:
        table_contract = list(filter(None, table_contract))
        print(table_contract)
        sys.stdout.flush()
        if table_contract != None:
            cur.executemany(contract, table_contract)
        else:
            pass
    else:
        pass



