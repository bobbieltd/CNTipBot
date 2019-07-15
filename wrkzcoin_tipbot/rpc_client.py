from typing import Dict
from uuid import uuid4

import aiohttp
import asyncio
import json

from config import config

import sys
sys.path.append("..")


class RPCException(Exception):
    def __init__(self, message):
        super(RPCException, self).__init__(message)

async def call_aiohttp_wallet_original(method_name: str, coin: str, payload: Dict = None) -> Dict:
    full_payload = {
        'params': payload or {},
        'jsonrpc': '2.0',
        'id': str(uuid4()),
        'method': f'{method_name}'
    }
    url = get_wallet_rpc_url(coin)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=full_payload, timeout=8) as response:
            if response.status == 200:
                res_data = await response.read()
                res_data = res_data.decode('utf-8')
                await session.close()
                decoded_data = json.loads(res_data)
                result = decoded_data['result']
                print(" RPC Original finished : "+res_data);
                return result
            else:
                print(" RPC Original Error status : "+response.status);
                return None
                
async def call_aiohttp_wallet(method_name: str, coin: str, payload: Dict = None) -> Dict:
    coin_family = getattr(getattr(config,"daemon"+coin),"coin_family","TRTL")
    indexMajor = 0

    if coin_family == "XMR" and method_name == "getBalance":
        method_name = "get_balance"
        if payload is not None and payload['address'] is not None:
            indices = await call_aiohttp_wallet_original('get_address_index', coin, payload=payload)
            indexMajor = indices['index']['major']
            payload["account_index"] = indexMajor
            payload["address_indices"] = [indices['index']['minor']]

    if coin_family == "XMR" and method_name == "getAddresses":
        method_name = "get_address"

    if coin_family == "XMR" and method_name == "sendTransaction":
        method_name = "transfer"
        indices = await call_aiohttp_wallet_original('get_address_index', coin, payload={"address":payload["addresses"]})
        indexMajor = indices['index']['major']
        payload["account_index"] = indexMajor
        payload["subaddr_indices"] = [indices['index']['minor']]
        payload["destinations"] = payload["transfers"]
        payload["priority"] = 0
        payload["mixin"] = payload["anonymity"]
        payload["get_tx_key"] = True
        payload["unlock_time"] = 0
        payload["payment_id"] = payload["paymentId"]

    full_payload = {
        'params': payload or {},
        'jsonrpc': '2.0',
        'id': str(uuid4()),
        'method': f'{method_name}'
    }
    url = get_wallet_rpc_url(coin)
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=full_payload, timeout=8) as response:
            if response.status == 200:
                res_data = await response.read()
                res_data = res_data.decode('utf-8')
                await session.close()
                decoded_data = json.loads(res_data)
                result = decoded_data['result']
                print(" RPC finished : "+res_data);
                if coin_family == "XMR" and method_name == "get_balance":
                    result['availableBalance'] = result["per_subaddress"][0]['unlocked_balance']
                    result['lockedAmount'] = result["per_subaddress"][0]['balance']-result["per_subaddress"][0]['unlocked_balance']
                if coin_family == "XMR" and method_name == "get_address":
                    resultReformat = {'addresses' : []}
                    for address in result["addresses"]:
                        resultReformat['addresses'].append(address["address"])
                    result = resultReformat
                if coin_family == "XMR" and method_name == "transfer":
                    getattr(config,"daemon"+coin).fee = result["fee"]
                    result["transactionHash"] = result["tx_hash"]
                if coin_family == "XMR":
                    print(coin+" RPC Result from XMR family: "+json.dumps(result))
                return result
            else:
                print(" RPC Error status : "+response.status);
                return None

async def call_doge_ltc(method_name: str, coin: str, payload: str = None) -> Dict:
    headers = {
        'content-type': 'text/plain;',
    }
    if payload is None:
        data = '{"jsonrpc": "1.0", "id":"'+str(uuid4())+'", "method": "'+method_name+'", "params": [] }'
    else:
        data = '{"jsonrpc": "1.0", "id":"'+str(uuid4())+'", "method": "'+method_name+'", "params": ['+payload+'] }'
    url = None
    if coin.upper() == "DOGE":
        url = f'http://{config.daemonDOGE.username}:{config.daemonDOGE.password}@{config.daemonDOGE.host}:{config.daemonDOGE.rpcport}/'
    elif coin.upper() == "LTC":
        url = f'http://{config.daemonLTC.username}:{config.daemonLTC.password}@{config.daemonLTC.host}:{config.daemonLTC.rpcport}/'
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data, timeout=8) as response:
            if response.status == 200:
                res_data = await response.read()
                res_data = res_data.decode('utf-8')
                await session.close()
                decoded_data = json.loads(res_data)
                return decoded_data['result']


def get_wallet_rpc_url(coin: str = None):
    return "http://"+getattr(config,"daemon"+coin,config.daemonWRKZ).wallethost+":"+str(getattr(config,"daemon"+coin,config.daemonWRKZ).walletport)\
    + '/json_rpc'

