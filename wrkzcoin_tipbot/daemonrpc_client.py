from typing import Dict
from uuid import uuid4

import rpc_client
import json
import aiohttp
import asyncio

import sys
sys.path.append("..")
from config import config

class RPCException(Exception):
    def __init__(self, message):
        super(RPCException, self).__init__(message)


async def getWalletStatus(coin: str):
    info = {}
    return await rpc_client.call_aiohttp_wallet('getStatus', coin)


async def getDaemonRPCStatus(coin: str):
    if (coin == "DOGE") or (coin == "LTC"):
        result = await rpc_client.call_doge_ltc('getinfo', coin)
    return result


async def gettopblock(coin: str):
    coin_family = getattr(config,"daemon"+coin,"daemonWRKZ").coin_family;
    if coin_family == "XMR":
        rpc_params = {
            'jsonrpc': '2.0',
            'method': 'getlastblockheader',
            'params': []
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(get_daemon_rpc_url(coin)+'/json_rpc', json=rpc_params, timeout=8) as response:
                if response.status == 200:
                    res_data = await response.json()
                    await session.close()
                    return res_data['result']
    else:
        result = await call_daemon('getblockcount', coin)
        full_payload = {
            'jsonrpc': '2.0',
            'method': 'getblockheaderbyheight',
            'params': {'height': result['count'] - 1}
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(get_daemon_rpc_url(coin)+'/json_rpc', json=full_payload, timeout=8) as response:
                if response.status == 200:
                    res_data = await response.json()
                    await session.close()
                    return res_data['result']


async def call_daemon(method_name: str, coin: str, payload: Dict = None) -> Dict:
    full_payload = {
        'params': payload or {},
        'jsonrpc': '2.0',
        'id': str(uuid4()),
        'method': f'{method_name}'
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(get_daemon_rpc_url(coin)+'/json_rpc', json=full_payload, timeout=8) as response:
            if response.status == 200:
                res_data = await response.json()
                await session.close()
                return res_data['result']


def get_daemon_rpc_url(coin: str = None):
    return "http://"+getattr(config,"daemon"+coin,"daemonWRKZ").host+":"+str(getattr(config,"daemon"+coin,"daemonWRKZ").port)

