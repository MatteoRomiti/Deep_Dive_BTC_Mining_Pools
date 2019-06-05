from IPython.display import HTML
from os import listdir
import random
import codecs
import re
import numpy as np
from operator import add
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import operator
import urllib
import json
import sys, os
import seaborn as sns
from time import time, sleep
import networkx as nx
from blockchain import blockexplorer as bx

###################### CONSTANTS ######################

secs_in_day = 60*60*24
secs_in_2weeks = secs_in_day*14

known_pools = json.load(open('./dataset/blockchain.info_2019-01-28.json'))
known_pool_addresses = list(known_pools['payout_addresses'].keys())
# print('Number of known addresses', len(known_pool_addresses))
pools_names = set(known_pools['coinbase_tags'].keys()) # strings that may appear in coinbase scriptSig
f2pool_tag1 = '.xf0.x9f.x90.x9f' # 🐟 
# f2pool_tag2 = 'Mined by '
f2pool_tag3 = '.xe4.xb8.x83.xe5.xbd.xa9.xe7.xa5.x9e.xe4.xbb.x99.xe9.xb1.xbc' # 七彩神仙鱼
retrieved_tags = ['Bitcoin-Ukraine.com.ua/', '/mining.bitcoinaffiliatnetwork.com/', 'dpool', 'poolin', 'tigerpool', 'masterpool', 'bitfarms', 'sigmapool', 'rawpool', 'okminer', 'prohashing', 'btcc', 'BitClub Network', '175btc', 'Tangpool', 'AntPool', '50btc.com', 'viabtc.com', '/canoepool/', '/E2M & BTC.TOP/', 'Bixin', 'BTPOOL', 'MiGPool', 'TangPool', 'B2Pool', 'GIVE-ME-COINS.com', '8baochi.com', 'Bixin', 'CoiniumServ', 'HAOBTC', 'haobtc', 'ckpool', 'pool.mkalinin.ru', 'p2pool Sucks', 'bitfury_pool', 'BTCC', 'BW Pool', 'Follow the white rabbit', 'nmcbit', 'coinpool', 'btcpoolman', 'P2PCOIN', 'GBMiners', f2pool_tag1, f2pool_tag3]
pools_names.remove('/八宝池 8baochi.com/')
blockchain_info_names = pools_names
blockchain_info_names.add('8baochi.com/')
clean_pools_names = set()
for name in pools_names:
    clean_pools_names.add(name.replace('/', ''))
for tag in retrieved_tags:
    clean_pools_names.add(tag)
specific_string = '|'.join(clean_pools_names)
specific_pattern = re.compile(specific_string, re.IGNORECASE)
blockchain_info_string = '|'.join(blockchain_info_names)
blockchain_info_pattern = re.compile(blockchain_info_string)
general_strings = ['pool', '..\.com', 'BTC', 'mine', 'Bit', 'coin']
general_string = '|'.join(general_strings)
general_pattern = re.compile(general_string, re.IGNORECASE)

###################### TRANSFORM FUNCTIONS ######################

def ts2date(ts):
    return datetime.datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')

def height2hash(height):
    # from height of a block to its hash
    bh = bx.get_block_height(height)
    for b in bh:
        if b.main_chain:
            return b.hash

def height2coinbase_text(h):
    success = False # to get the requested block associated to height
    while not success:
        try:
            block = block_hash2dict(height2hash(h)) # blockexplorer and graphsense needed
            success = True
        except:
            sleep(.1)
    coinbase_hash = block['tx'][0]
    coinbase = tx_hash2dict(coinbase_hash)
    text = get_coinbase_text(coinbase['vin'][0]['coinbase'])
    return text

def url2dict(url):
    res = None
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
        res = json.loads(html.decode('utf-8'))
    except:
        print(url)
    return res

    
def block_hash2dict(block_hash):
    # get a json no-tx-details (tx hash only) block from graphsense (VPN required) by its hash
    return url2dict('https://blockchain.info/rawblock/' + block_hash)

def tx_hash2dict(tx_hash, graphsense=False):
    if graphsense:
        return url2dict('http://localhost:9000/btc/tx/' + tx_hash)
    else:
        return url2dict('https://blockchain.info/rawtx/' + tx_hash)

def list2short_string(lst, n=20, sep='_'):
    string = sep.join(set(lst))
    return limit_string(string, n)

def addresses2hash(lst):
    max_num_addresses = 4
    if len(lst) < max_num_addresses:
        return '_'.join([addr[:4] for addr in lst])
    else:
        return str(abs(hash('_'.join([addr for addr in lst]))))[:15]

def address2unknown_blocks(addresses, addr):
    unknown_blocks = []
    for i in range(len(addresses[addr]['blocks'])):
        if addresses[addr]['pool_tags'][i] == 'unknown':
            unknown_blocks.append(addresses[addr]['blocks'][i])
    return unknown_blocks

def height2coinbase_text(h):
    success = False # to get the requested block associated to height
    while not success:
        try:
            block = block_hash2dict(height2hash(h)) # blockexplorer and graphsense needed
            success = True
        except:
            sleep(.1)
    print('Block retrieved')
    coinbase_hash = block['tx'][0]['hash']
    coinbase = tx_hash2dict(coinbase_hash)
    text = get_coinbase_text(coinbase['inputs'][0]['script'])
    return text

def limit_string(s, l):
    s = str(s)
    if len(s) > l:
        s = s[:l]
    return s

###################### GET FUNCTIONS ######################

def get_coinbase_text(script_sig):
    # script_sig is a hex version of the coinbase text
    return str(codecs.decode(str(script_sig), "hex")) # TODO: fix problem with chinese and russian words

def get_output_addresses_and_BTC(outputs, spent_needed=True, height=0):
    # outputs is a list of dicts
    # returns a dict with addresses as key and a list of their [BTC, spent] as value
    addresses_and_BTC = dict()
    for out in outputs:
        if out['value']:
            try:
                if 'scriptPubKey' in out.keys() and 'addresses' in out['scriptPubKey'].keys():
                    if spent_needed and 'spent' in out.keys():
                        # be careful, the json has spent=true/false and not True/False as in python
                        addresses_and_BTC[out['scriptPubKey']['addresses'][0]] = [out['value'], out['spent']]
                    else:
                        addresses_and_BTC[out['scriptPubKey']['addresses'][0]] = out['value']
                else:
                    if 'addr' in out.keys():
                        if spent_needed and 'spent' in out.keys():
        #                     addresses_and_BTC[out['addr']] = [out['value'], out['spent']]
                            if out['addr'] not in addresses_and_BTC.keys():
                                addresses_and_BTC[out['addr']] = []
                            addresses_and_BTC[out['addr']].append([out['value'], out['spent']])
                        else:
        #                     addresses_and_BTC[out['addr']] = [out['value']]
                            if out['addr'] not in addresses_and_BTC.keys():
                                addresses_and_BTC[out['addr']] = []
                            addresses_and_BTC[out['addr']].append([out['value']])
            except:
                pass
#                 print('Unable to decode output address at block ' + str(height) + ' with value: ' + str(out['value']))
    return addresses_and_BTC

def get_coinbase_addresses_and_BTC(block, spent_needed=True, graphsense=False):
    # block is a dict
    # returns a dict with addresses as key and a list of their [BTC, spent] as value
#     cb = tx_hash2dict(block['tx'][0]) # from graphsense
        cb = block['tx'][0]
        if graphsense:
            return get_output_addresses_and_BTC(cb['vout'], spent_needed, height=block['height'])
        else:
            return get_output_addresses_and_BTC(cb['out'], spent_needed)
            
        
def get_pool_tag(block, blockchain_info_only=False):
    # block is a dict
    # returns a str
    # cb = tx_hash2dict(block['tx'][0])
    cb = block['tx'][0]
    msg = get_coinbase_text(cb['inputs'][0]['script'])
    pool_tag = get_tag_from_coinbase(msg, blockchain_info_only)
    return pool_tag

def get_tag_from_coinbase(msg, blockchain_info_only=False):
    # text is a str where there is a possible pool tag
    # returns the tag of the pool or unknown
    global specific_pattern, blockchain_info_pattern

    pool_name = 'unknown'
    if blockchain_info_only:
        match = blockchain_info_pattern.findall(msg)
    else:
        match = specific_pattern.findall(msg)
    if match: 
        pool_name = '_'.join(match)
    return pool_name

def update_data(up_to_block, blocks_file_path='./dataset/blocks.json', addresses_file_path='./dataset/addresses.json',):
    # blockchain.blockexplorer and graphsense required
    try:
        with open(blocks_file_path, 'r') as fp:
            blocks = json.load(fp)
        with open(addresses_file_path, 'r') as fp:
            addresses = json.load(fp)
        last_height = blocks['last_height'] # last block analysed 
    except:
        print('Creating data from scratch.')
        blocks = dict()
        addresses = dict()
        last_height = 0
    print('Last block included in data:', last_height)

    # avoid orphans
    done = False
    while not done:
        try:
            for b in bx.get_block_height(up_to_block):
                if b.main_chain:
                    latest_block = b
                    done = True
        except:
            sleep(.1)
    latest_block_hash = latest_block.hash
    latest_height = latest_block.height
    success_till = latest_height # write success_till height only after the whole analysis
    
    if latest_height > last_height: # the genesis block is not considered
        latest_block = block_hash2dict(latest_block_hash) # dict with notxdetails
        while latest_height > last_height:
            print('Analysing block:', latest_height, end='\r')
            previous_block = block_hash2dict(latest_block['prev_block'])
            
            blocks[latest_height] = dict()
            blocks[latest_height]['time'] = latest_block['time'] # int
            pool_tag = get_pool_tag(latest_block) # string
            blocks[latest_height]['pool_tag'] = pool_tag
            coinbase_addresses_and_BTC = get_coinbase_addresses_and_BTC(latest_block, spent_needed=False) # dict with addresses as key and BTC as value
            coinbase_addresses = list(coinbase_addresses_and_BTC.keys())
            blocks[latest_height]['addresses'] =  coinbase_addresses
            blocks[latest_height]['num_addresses'] = len(blocks[latest_height]['addresses'])
            
            for address in coinbase_addresses:
                BTC = coinbase_addresses_and_BTC[address]
                if address not in addresses.keys():
                    addresses[address] = dict()
                    addresses[address]['blocks'] = []
                    addresses[address]['pool_tags'] = []
                    addresses[address]['BTC'] = 0
                addresses[address]['blocks'].append(latest_height)
                
                addresses[address]['pool_tags'].append(pool_tag)
                addresses[address]['BTC'] += sum([el[0] for el in BTC])
            latest_block = previous_block
            latest_height = latest_block['height']
        blocks['last_height'] = success_till
        addresses['last_height'] = success_till 
        print()
        print('Writing data...')
        data_dir = './dataset/'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        with open(blocks_file_path, 'w') as fp:
            json.dump(blocks, fp)
        with open(addresses_file_path, 'w') as fp:
            json.dump(addresses, fp)
        
        print('Done.')

    else:
        print('Data already up-to-date.')
    
def get_updated_data(blocks_needed=True, addresses_needed=True, blocks_file_path='./dataset/blocks.json', addresses_file_path='./dataset/addresses.json'):
    # make sure you run update_data() first
    blocks, addresses = None, None
    try:
        if blocks_needed:
            with open(blocks_file_path, 'r') as fp:
                blocks = json.load(fp)
        if addresses_needed:
            with open(addresses_file_path, 'r') as fp:
                addresses = json.load(fp)
            if 'last_height' in addresses.keys():
                del addresses['last_height']
    except:
        print('No data in specified paths.')
    return blocks, addresses

def get_multi_output_blocks(last_block_hash, n_blocks=100, min_n_outputs=3, \
                            prnt=False, print_coinbase_text=False, print_addresses=False, print_value=False):
    # go back n_blocks blocks in the blockchain from last_block_hash and get blocks 
    # with at least min_n_outputs outputs
    # returns a dict with height as key and number of outputs as value
    # using graphsense API
    multi_output = dict()
    for n in range(n_blocks):
        block = block_hash2dict(last_block_hash)
        prev_block_hash = block['previousblockhash']
        coinbase_hash = block['tx'][0]
        coinbase = tx_hash2dict(coinbase_hash)
        n_outputs = len(coinbase['vout'])
        if n_outputs > min_n_outputs:
            multi_output[block['height']] = n_outputs
            if prnt:
                print('-'*60)
                print('Height', block['height'])
                print('Number of outputs:', n_outputs)
                for out_n in range(n_outputs):
                    value = coinbase['vout'][out_n]['value']
                    if value:
                        if print_value:
                            print('Value: ', value)
                        if print_addresses:
                            print('Payout addresses: ', coinbase['vout'][out_n]['scriptPubKey']['addresses'])
                        if print_coinbase_text:
                            print('Coinbase text:', get_coinbase_text(coinbase['vin'][0]['coinbase']))
        last_block_hash = prev_block_hash
    return multi_output

def get_address_sent_txs(address, offset, limit=50):
    success = False
    # returns a list of txs (dicts) where address is in the inputs 
    url = 'https://blockchain.info/address/' + address + '?format=json&offset=' + str(50*offset) + \
                '&limit=' + str(limit) + '&sort=0&filter=1'
    while not success:
        try:
            result = url2dict(url)['txs']
            success = True
        except Exception as e:
            print(url)
            print_error_and_sleep(e, 1)
    return result

def get_address_received_txs(address, offset, limit=50):
    result = 0
    # returns a list of txs (dicts) where address is in the outputs
    url = 'https://blockchain.info/address/' + address + '?format=json&offset=' + str(50*offset) + \
                '&limit=' + str(limit) + '&sort=0&filter=2'
    result = url2dict(url)['txs']
    # print(url)
    return result

def get_next_step(address, BTC, coinbase_time):
    # how address spent BTC, assuming BTC was actually spent
    # returns a dict with output addresses as key and [BTC, spent] as value
#     SAT = BTC*10**8
    SAT = BTC
    addresses_SAT = dict()
    offset = 0
    while True:
#         print('offset', offset)
        # return only when next step is found
        tx_list = get_address_sent_txs(address, offset) # list of txs (dicts) where address is among the inputs 
        for tx in tx_list:
            for i in tx['inputs']:
                # find tx where the same BTC is spent and the time is greater than the coinbase tx
                if i['prev_out']['value'] == SAT and i['prev_out']['addr'] == address \
                and tx['time'] > coinbase_time:
                # get output address and BTC
#                     print(tx['hash'])
                    return get_output_addresses_and_BTC(tx['out'])
        # this is not the proper way, but it's faster 
        # we should start from the block time and go forward 
        # while here we go from current time backward
        offset += 1  

###################### PRINT FUNCTIONS FOR BLOCKEXPLORER ######################

def print_block(b, coinbase_only=True):
    # print a Block object b retrieved with blockexplorer from blockchain module
    print('#'*10, 'BLOCK', '#'*10)
    print('hash:', b.hash)
    print('version:', b.version)
    print('previous_block:', b.previous_block)
    print('merkle_root:', b.merkle_root)
    print('time:', b.time, ts2date(b.time))
    print('bits:', b.bits)
    print('fee:', b.fee)
    print('nonce:', b.nonce)
    print('n_tx:', b.n_tx)
    print('size:', b.size)
    print('block_index:', b.block_index)
    print('main_chain:', b.main_chain)
    print('height:', b.height)
    print('received_time:', b.received_time, ts2date(b.received_time))
    print('relayed_by:', b.relayed_by)
#         print('transactions:', b.transactions)
    print_array_of_txs(b.transactions, coinbase_only)

def print_array_of_blocks(array, coinbase_only=True):
    for b in array: 
        print_block(b, coinbase_only)

def print_input(i):
    # print an Input object retrieved with blockexplorer from blockchain module
    print('#'*10, 'INPUT', '#'*10)
    coinbase = False
    try:
        print('n', i.n)
        print('value', i.value/(10**8))
        print('address', i.address)
        print('tx_index', i.tx_index)
        print('type', i.type)
    except:
        print('COINBASE')
        coinbase = True
    try:
        print('script', i.script)
    except:
        print('No script attribute')
    print('script_sig', i.script_sig)
    if i.script_sig:
        print('Coinbase message:', get_coinbase_text(i.script_sig))
    print('sequence', i.sequence)

def print_array_of_inputs(arr):
    for i in arr:
        print_input(i)

def print_output(o):
    # print an Output object retrieved with blockexplorer from blockchain module
    print('#'*10, 'OUTPUT', '#'*10)
    print('n', o.n)
    print('value', o.value/(10**8))
    print('address', o.address)
    print('tx_index', o.tx_index)
    print('script', o.script)
    print('spent', o.spent)

def print_array_of_outputs(arr):
    for i in arr:
        print_output(i)

def print_tx(tx):
    # print a Transaction object retrieved with blockexplorer from blockchain module
    print('#'*10, 'TX', '#'*10)
    print('double_spend:', tx.double_spend)
    print('block_height:', tx.block_height)
    print('time:', tx.time, ts2date(tx.time))
    print('relayed_by:', tx.relayed_by)
    print('hash:', tx.hash)
    print('tx_index:', tx.tx_index)
    print('version:', tx.version)
    print('size:', tx.size)
    print_array_of_inputs(tx.inputs)
    print_array_of_outputs(tx.outputs)

def print_array_of_txs(arr, coinbase_only=True):
    if coinbase_only:
        print_tx(arr[0])
    else:
        for tx in arr:
            print_tx(tx)

def print_error_and_sleep(e, n=2):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    sleep(n)
    
def get_address_perc(start_time, end_time, blocks_file_path='./dataset/blocks.json'):
#     returns a dict with addresses as key and their percentage as value
    with open(blocks_file_path, 'r') as fp:
        blocks = json.load(fp)
    address_nMinedBlocks = dict()
    tot_blocks_in_period = 0
    apparent_blocks_in_period = 0 # one block may have multiple output addresses
    for h in blocks.keys():
        if h != 'last_height':
            t = blocks[h]['time'] # block timestamp 
            addresses = blocks[h]['addresses']
            if t >= start_time and t <= end_time:
                if len(addresses) > 3:
                    print('Block', h, 'has', len(addresses), 'output addresses')
                for addr in addresses: 
                    apparent_blocks_in_period += 1
                    if addr not in address_nMinedBlocks.keys():
                        address_nMinedBlocks[addr] = 1
                    else:
                        address_nMinedBlocks[addr] += 1
                tot_blocks_in_period += 1
    address_perc = dict()
    for addr in address_nMinedBlocks.keys():
        address_perc[addr] = 100*address_nMinedBlocks[addr]/apparent_blocks_in_period
#     address_perc_list = list(address_perc.items())
#     address_perc_list.sort(key=lambda x: -x[1])
#     return address_perc_list
    return address_perc

####################### FUNCTIONS FOR ANTPOOL BEHAVIOR ANALYSIS #######################
    
def find_richest_address(outputs):
    """
    Arguments:
    - outputs: list of ouput objects, outputs of a transaction
    
    Purpose:
    Find the address that received the biggest amount of BTC among the outputs
    
    Outputs:
    richest_address: address object, address that received the biggest amount of BTC among the outputs
    richest_address_str: str, address of richest_address object
    max_BTC: float, biggest amount of BTC received by richest_address
    spent: Boolean, whether max_BTC is spent
    """
    max_BTC = 0
    richest_address_idx = 0
    richest_address_str = ''
    spent = False
    for i, out in enumerate(outputs): 
        BTC = out.value/10**8
        if BTC > max_BTC:
            max_BTC = BTC
            richest_address_idx = i
            spent = out.spent
    failed = True
    while failed:
        try: # reading an address may be too slow
            richest_address_str = outputs[richest_address_idx].address
            failed = False
        except Exception as e:
            print_error_and_sleep(e, 2)
#     print(richest_address_str)
    richest_address = bx.get_address(richest_address_str) 
    return richest_address, richest_address_str, max_BTC, spent


def get_leaf_from_root(root_tx_hash, threshold=2, max_depth=100):
    """Starting from a tx hash, this function follows the flow of the biggest amount of BTC in the outputs.
    It stops when:
    - the biggest amount of BTC received is not spent
    - the biggest amount of BTCis below 'threshold' BTC
    - 'max_depth' transactions are reached
    
    :param str root_tx_hash: hash of a transaction you suppose to be the root of a tree to split a big amount of BTC
    :param int max_depth: how many splitting transactions to check
    :param int thershold: the highest UTXO at each level of depth must be greater than this number
    
    :return 
    - richest_address_str: str, address of the leaf (received the last big amount of BTC)
    - max_BTC: float, last big amount of BTC received by the leaf
    """
    root_tx = bx.get_tx(root_tx_hash) 
    depth_reached = 0
    for depth in range(max_depth):
        depth_reached = depth
#         print('Depth:', depth, end='\r')
        if len(root_tx.outputs) != 101: # if we know it happens often, don't print it 
            print(str(depth) + '-' + str(len(root_tx.outputs)), end=' ')
#         print(str(depth))
#         print('number of outputs:', len(root_tx.outputs))
        richest_address, richest_address_str, max_BTC, spent = find_richest_address(root_tx.outputs)
        if max_BTC > threshold: # minimum threshold for richest address
            if len(richest_address.transactions) > 2:
                print('Richest address has ' + str(len(richest_address.transactions)) + ' transactions. Check manually what is going on. Address:', richest_address_str)
            if spent:
                # look at next level of depth and find transaction with same UTXOs
                for tx in richest_address.transactions: 
                    if len(tx.inputs) == 1: # one input only otherwise we may mix roots and leaves
                        same_BTC = False
                        origin_address_found_in_input = False
                        for inpt in tx.inputs: # we loop only once, yes
                            if inpt.address == richest_address_str and inpt.value/10**8 == max_BTC:
                                origin_address_found_in_input = True # to ignore txs where the origin address is not an input
                                same_BTC = True # to avoid mixing UTXOs
                        if origin_address_found_in_input and same_BTC:
                            next_root_tx = tx # look for next big address in output
                if next_root_tx != root_tx:
                    root_tx = next_root_tx
                else:
                    print('Leaf reached because unable to find next transaction')
                    break
            else:
                print('Leaf reached because of unspent BTC')
                break
        else: 
            print('Below threshold')
            break
    print('Depth reached:', depth_reached, ', leaf:', richest_address_str, ', BTC:', max_BTC)
    print()
    return richest_address_str, max_BTC


########################### COINBASE FLOW ###########################
        
def plot_coinbase_flow(height, max_steps, tag):
    # GRAPHSENSE REQUIRED
    # height is an int specifying the block to plot
    # max_steps is an int for how many levels (depth) we want to plot from the coinbase
    success = False # to get the requested block associated to height
    while not success:
        try:
            block = block_hash2dict(height2hash(height)) # blockexplorer and graphsense needed
            success = True
        except:
            sleep(.1)
    block_time = block['time']
    cb = block['tx'][0] # coinbase hash only
    cb_tx = tx_hash2dict(cb) 
    # get all coinbase output addresses and their [BTC, spent]
    coinbase_addresses_BTC = get_coinbase_addresses_and_BTC(block, graphsense=False, spent_needed=True)
    print('COINBASE ADDRESSES', coinbase_addresses_BTC)
    G = nx.Graph()
    addresses_BTC_steps = [coinbase_addresses_BTC] # step 0
    
    for cb_addr in coinbase_addresses_BTC.keys(): # depth first
        node_name = cb_addr + '_0' 
        G.add_node(node_name, text=node_name) # add coinbase address to graph
#         print('coinbase addresses', cb_addr, coinbase_addresses_BTC[cb_addr])
#         print('coinbase address', cb_addr)
    step_zero = 0
    G, addresses_BTC_steps = foreach_addr_do_next_step(addresses_BTC_steps, G, block_time, step_zero, max_steps)
#         if coinbase_addresses_BTC[cb_addr][1]: # spent or not
# #             print('next step')
#             for step in range(steps):
#                 next_addresses_BTC.append(get_next_step(cb_addr, coinbase_addresses_BTC[cb_addr][0], block_time))
#     #             print(next_addresses_BTC)
#                 for next_addr in next_addresses_BTC[step].keys():
#     #                 print(next_addr)
#                     G.add_node(next_addr, text=next_addr)
#                     G.add_edge(cb_addr, next_addr)
#                 cb_addr
    color_map = []
    steps_colors = ['orange', 'green', 'blue', 'black']
    labels = nx.get_node_attributes(G, 'text')
    node_size = []
    factor = 10**6
    for node in G:
        color_map.append(steps_colors[int(node[-1])])
#         found = False
#         # a coinbase address can also be a change address
#         # but here we ignore the change BTC
#         for step in range(len(addresses_BTC_steps)):
#             if node in addresses_BTC_steps[step].keys():
#                 color_map.append(steps_colors[step])
#                 found = True
#         if not found:
#             color_map.append('red')
#                 BTC_sum = 0
#                 for txo in coinbase_addresses_BTC[node]:
#                     BTC_sum += txo[0]
#                 node_size.append((BTC_sum/factor)**(1/2))
#         else:
#             for step in range(1, max_steps):
#                 if node in addresses_BTC_steps[step].keys():
#                     BTC_sum = 0
#                     for txo in addresses_BTC_steps[step][node]:
#                         BTC_sum += txo[0]
#                     node_size.append((BTC_sum/factor)**(1/2))
#                     color_map.append(steps_colors[step])
    plt.figure(figsize=(40,40))
#     nx.draw(G)
    nx.draw(G, node_color=color_map)
#     nx.draw(G, node_color=color_map, node_size=node_size)
    plt.title('Block: ' + str(height) + ' Tag: ' + tag)
    plt.savefig('./images/coinbase_flow_' + str(height) +'.pdf')
    plt.show()

def foreach_addr_do_next_step(addresses_BTC_steps, G, block_time, step, max_steps):
    if step < max_steps:
        print('Step:', step)
        all_addresses_step = dict() # all the addresses in the next step
        for addr in addresses_BTC_steps[step].keys(): # addresses in current step
            txo_list = addresses_BTC_steps[step][addr] # list of [BTC, spent]
            for txo in txo_list:
                if txo[1]: # if spent
                    # how addr spent BTC: dict with output addresses as key and a list of 
                    # their [BTC (received from addr), spent] as value
                    one_address_step = get_next_step(addr, txo[0], block_time)
                    for next_addr in one_address_step.keys(): # iterate through the output addresses
                        if next_addr not in all_addresses_step.keys():
                            all_addresses_step[next_addr] = []
                        for rec_BTC_spent in one_address_step[next_addr]:
                            all_addresses_step[next_addr].append(rec_BTC_spent) # all the received [BTC, spent] of next_addr
                        node_name = next_addr + '_' + str(step+1)
                        prev_node_name = addr + '_' + str(step)
                        G.add_node(node_name, text=node_name)
                        G.add_edge(prev_node_name, node_name)
                
        addresses_BTC_steps.append(all_addresses_step)
        return foreach_addr_do_next_step(addresses_BTC_steps, G, block_time, step+1, max_steps)
    else:
        return G, addresses_BTC_steps
    
########################## PIE CHART AND STACK PLOT ##########################

def print_pie(start_time, end_time, blocks_file_path='./dataset/blocks.json', group_by=''):
    # group_by: cluster_ID/payout_cluster_ID/''
    address_perc_dict = get_address_perc(start_time, end_time, blocks_file_path) # initial groupby address
    _, addresses = get_updated_data(blocks_needed=False)
    if group_by:
        grouped_perc_dict = my_groupby(address_perc_dict, addresses, group_by)
    else: # group_by address
        grouped_perc_dict = address_perc_dict        
    grouped_perc_list = list(grouped_perc_dict.items()) # dict to list with [key, value] as elements
    grouped_perc_list.sort(key=lambda x: -x[1])
    labels, values = [el[0] for el in grouped_perc_list], [el[1] for el in grouped_perc_list]
    plt.figure(figsize=(50,50))
    ax = plt.gca()
    patches, texts, autotexts = ax.pie(values, labels=labels, autopct='%1.1f%%', radius=0.3)
    plt.title('Start time: ' + ts2date(start_time) + ' End time: ' + ts2date(end_time), y=1.08, fontsize=70)
    plt.axis('equal')
    max_label_size = 20
    min_label_size = 10
    label_factor = 10
    if group_by:
        max_label_size = 30
        min_label_size = 20
        label_factor = 10
    for t,at in zip(texts, autotexts):
        fontsize_labels = max(min(label_factor*float(at.get_text()[:-1]), max_label_size), min_label_size)
        fontsize_perc = max(12*float(at.get_text()[:-1]), 25)
        t.set_fontsize(fontsize_labels) # labels
        at.set_fontsize(fontsize_perc) # percentages
#     print(fontsize_labels, fontsize_perc)
    plt.savefig("images/pools_share_" + str(start_time) + "_" + str(end_time) + "_groupBy_" + group_by + ".pdf")
    plt.show()
    return grouped_perc_list

def my_groupby(address_perc_dict, addresses, group_by):
    # address_perc_dict: address as key, perc as value
    # group_by: 'cluster_ID', 'payout_cluster_ID', 'both'
    # returns:
    # grouped_perc_dict: cluster+associated_tags as key and the summed percentages as value
    
#     print('Addresses with same payout cluster but different graphsense cluster')
#     for addr1 in address_perc_dict.keys():
#         for addr2 in address_perc_dict.keys():
#             cl1 = addresses[addr1]['cluster_ID']
#             cl2 = addresses[addr2]['cluster_ID']
#             if cl1 == addr1:
#                 cl1 = 'none'
#             if cl2 == addr2:
#                 cl2 = 'none'
#             if addresses[addr1]['cluster_ID'] != addresses[addr2]['cluster_ID'] and (cl1 != 'none' and cl2 != 'none'):
#                 if addresses[addr1]['payout_cluster_ID'] == addresses[addr2]['payout_cluster_ID']:
#                     print(addr1, addr2)

    if group_by == 'both':
        return groupby_both(address_perc_dict, addresses)
    grouped_perc_dict = dict()
    cluster_tags_labels = dict()
    for addr in address_perc_dict.keys():
        cluster_to_use = addresses[addr][group_by]
        pool_tags = list2short_string(addresses[addr]['pool_tags'], sep='+')        
        if cluster_to_use not in cluster_tags_labels.keys():
            cluster_tags_labels[cluster_to_use] = set() # tags with this clusterID will be appended here
        if cluster_to_use not in grouped_perc_dict.keys():
            grouped_perc_dict[cluster_to_use] = 0
        cluster_tags_labels[cluster_to_use].add(list2short_string(addresses[addr]['pool_tags']))
        grouped_perc_dict[cluster_to_use] += address_perc_dict[addr] # two addresses with the same clusterID will sum their perc
    grouped_perc_dict = {limit_string(clusterID, 8) + '\n' + '\n'.join(cluster_tags_labels[clusterID]) : grouped_perc_dict[clusterID] for clusterID in grouped_perc_dict.keys()}
    return grouped_perc_dict

def groupby_both(address_perc_dict, addresses):
    grouped_perc_dict = dict()
    payoutCluster_cluster_perc_list = []
    cluster_tags_labels = dict()
    for addr in address_perc_dict.keys():
        cluster_ID = addresses[addr]['cluster_ID']
        pool_tags = list2short_string(addresses[addr]['pool_tags'], sep='+')  
        payout_cluster_ID = addresses[addr]['payout_cluster_ID']
        if cluster_ID == addr and pool_tags == 'unknown':     
            cluster_ID ='none' # to group addresses that mined together and have no tag
        if cluster_ID not in cluster_tags_labels.keys():
            cluster_tags_labels[cluster_ID] = set()
        cluster_tags_labels[cluster_ID].add(pool_tags)
        payoutCluster_cluster_perc_list.append([payout_cluster_ID, cluster_ID, address_perc_dict[addr]])
    df = pd.DataFrame(payoutCluster_cluster_perc_list, columns=['payout_cluster_ID', 'cluster_ID', 'perc'])
    # addresses with same payout_cluster and cluster are summed
    gb1 = df.groupby(['payout_cluster_ID', 'cluster_ID']).sum().reset_index()
    # addresses with different payout_cluster_ID, but same cluster_ID are summed
    gb2 = gb1.groupby(['cluster_ID']).sum().reset_index()
    # addresses with same payout_cluster_ID, but different real graphsense cluster_ID (which should be rare)
    # will appear in different slices
    cluster_perc_list = gb2.values.tolist()
    grouped_perc_dict = {limit_string(el[0], 8) + '\n' + '\n'.join(cluster_tags_labels[el[0]]) : el[1] for el in cluster_perc_list}
    return grouped_perc_dict

def print_stack_plot(end_time, period_len, num_periods, save=False, threshold=0, legend=None, group_by='both'):
    # end_time: unix timestamp of the last block to be analysed
    # period_len: number of seconds of a period during which we compute the percentage of an address
    # num_periods: int, how many periods will be plotted, corresponds to the x axis
    # threshold: a cluster must have an average percentage >= than threshold for its non-zero percentages 
    # legend: 'address' to have addesses in legend, otherwise cluster_ID + pool_tagsin legend, used only when group_by is False
    # group_by: 'pool_tag' or 'cluster_ID or False for no clustering (address only)

    # WARNING: if threshold = 0, the legend will be very long
    # so it's better to plot a short period 

    start_time = end_time - period_len 
    # start_time = 1231006505 # minimum
    grouped_perc_list = []
    start = start_time # moving start
    end = end_time # moving end
    _, addresses = get_updated_data()
    for period in range(num_periods):
        print('Computing period', period, end='\r')
        # grouped_perc_list[n] is a dict with address as key and perc as value for period n
        address_perc_dict = get_address_perc(start, end)
        grouped_perc_dict = my_groupby(address_perc_dict, addresses, group_by)
        grouped_perc_list.append(grouped_perc_dict) 
        # go to previous period
        end = start 
        start -= period_len
    grouped_perc_list = grouped_perc_list[::-1] # invert list because we went back in time
#     all_addresses = set()
#     for n in range(num_periods):
#         for k in grouped_perc_list[n].keys():
#             all_addresses.add(k)
#     y_dict = dict()
#     for addr in all_addresses:
#         y_dict[addr] = [grouped_perc_list[period][addr] if addr in grouped_perc_list[period].keys() else 0 for period in range(num_periods)]
#         if threshold:
#             if sum(y_dict[addr]) < threshold:
#                 y_dict.pop(addr, None)
    y_dict = dict() # cluster_associatedTags as key and list of percentages as values
    for n, dct in enumerate(grouped_perc_list): # list of dicts, one for each period, 
        for k in dct.keys(): # cluster_tags in period n
            if k not in y_dict.keys():
                y_dict[k] = [0 for i in range(num_periods)]
            y_dict[k][n] = dct[k]
    y_th_dict = dict()
    if threshold:
        for k in y_dict.keys():
            
            s = sum(y_dict[k]) # sum all the percentages in the 
            ave = s/len([el for el in y_dict[k] if el > 0])
            if ave > threshold:
#                 y_th_dict[str(round(sum(y_dict[k])/num_periods, 2)) + ' ' + k] = y_dict[k]
                y_th_dict[k] = y_dict[k]
            else:
                print('Below threshold:', str(k), s)
    else:
        y_th_dict = y_dict()        
    len_pal = len(y_th_dict.keys())
    if len_pal > 6: # use scale without duplicate colors
#         pal = sns.diverging_palette(255, 133, l=60, n=len_pal, center="dark")
#         pal = sns.color_palette('hls', len_pal)
        pal = sns.color_palette('hls')
    else:
        pal = sns.color_palette('hls')
    x = [period+1 for period in range(num_periods)]
    y = np.vstack([y_th_dict[k] for k in y_th_dict.keys()])
    plt.figure(figsize=(75,50))
    labels = [k for k in y_th_dict.keys()]
#     labels = [str(round(sum(y_th_dict[k])/num_periods, 2)) + ' ' + k for k in y_th_dict.keys()]
    plt.stackplot(x, y, labels=labels, colors=pal)
    # plt.stackplot(x, y, labels=labels)
    plt.grid()
    print('Number of stacks', len_pal)
    
    if len_pal < 50: # else legend too big to show 
        size = int(950/len_pal)
        print
        plt.legend(prop={'size': size})
    xticks = [ts2date(start + i*period_len) for i in range(num_periods)]
    print(xticks)
    ax = plt.gca()
    ax.yaxis.set_label_position('right')
    ax.yaxis.set_ticks_position('right')
    ax.yaxis.tick_right()
    plt.xticks(x, xticks)
    if save:
        plt.savefig('./images/stackplot_periodLen_' + str(period_len) + 'secs_end_' + str(end_time) + '_numPeriods_' + str(num_periods) + '_threshold_' + str(threshold) + '_groupBy_' + group_by + '_legend_' + legend + '.pdf')
    plt.show()
    return y_th_dict

def gini(array):
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom eq:
    # http://www.statsdirect.com/help/generatedimages/equations/equation154.svg
    # from:
    # http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
    # All values are treated equally, arrays must be 1d:
    array = array.flatten()
    if np.amin(array) < 0:
        # Values cannot be negative:
        array -= np.amin(array)
    # Values cannot be 0:
    array += 0.0000001
    # Values must be sorted:
    array = np.sort(array)
    # Index per array element:
    index = np.arange(1,array.shape[0]+1)
    # Number of array elements:
    n = array.shape[0]
    # Gini coefficient:
    return ((np.sum((2 * index - n  - 1) * array)) / (n * np.sum(array)))
