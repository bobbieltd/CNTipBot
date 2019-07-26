import traceback, pdb
from typing import List, Dict
import json
from uuid import uuid4
import rpc_client, addressvalidation
import aiohttp
import asyncio
import time
from config import config

import sys
sys.path.append("..")


async def registerOTHER(coin: str) -> str:
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"daemon"+COIN_NAME),"coin_family","TRTL")
    main_address = getattr(getattr(config,"daemon"+COIN_NAME),"DonateAddress")

    payload = {
        'label' : 'tipbot'
    }
    paymentid = None
    if coin_family == "XMR":
        paymentid = addressvalidation.paymentid(8)
    else:
        paymentid = addressvalidation.paymentid(32)
    integratedAddress = await addressvalidation.make_integrated(main_address, COIN_NAME, paymentid)
    reg_address = {}
    reg_address['main_address'] = main_address
    reg_address['int_address'] = integratedAddress
    reg_address['paymentid'] = paymentid
    
    # Avoid any crash and nothing to restore or import
    print('Wallet register: '+reg_address['int_address']+'=>base_address: '+reg_address['main_address']+" ,family: "+coin_family)
    # End print log ID,spendkey to log file
    return reg_address


async def getSpendKey(from_address: str, coin: str) -> str:
    coin_family = getattr(getattr(config,"daemon"+coin),"coin_family","TRTL");
    spendKey = ""
    payload = {
        'address': from_address
    }
    # index and not using spentKey with Monero
    if coin_family == "XMR":
        result = await rpc_client.call_aiohttp_wallet('get_address_index', coin, payload=payload)
        spendKey =  str(result['index']['major'])+","+str(result['index']['minor'])
    else:
        result = await rpc_client.call_aiohttp_wallet('getSpendKeys', coin, payload=payload)
        spendKey = result['spendSecretKey']

    return spendKey


async def send_transaction_donate(from_address: str, to_address: str, amount: int, coin: str) -> str:
    coin = coin.upper()
    payload = {
        'addresses': [from_address],
        'transfers': [{
            "amount": amount,
            "address": to_address
        }],
        'fee': get_tx_fee(coin),
        'anonymity': get_mixin(coin)
    }
    result = None
    result = await rpc_client.call_aiohttp_wallet('sendTransaction', coin, payload=payload)
    if result:
        if 'transactionHash' in result:
            return result['transactionHash']
    return result


async def send_transaction(from_address: str, to_address: str, amount: int, coin: str, acc_index: int = 0) -> str:
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"daemon"+COIN_NAME),"coin_family","TRTL")

    if from_address.upper() in ["TIPBOT","BOT"]: 
        from_address = getattr(getattr(config,"daemon"+COIN_NAME),"DonateAddress")
    else:
        print("It should be not sending from other source !!!")
        return None
    payload = {
        'addresses': [from_address],
        'transfers': [{
            "amount": amount,
            "address": to_address
        }],
        'fee': get_tx_fee(coin),
        'anonymity': get_mixin(coin)
    }

    result = await rpc_client.call_aiohttp_wallet('sendTransaction', coin, payload=payload)
    if result:
        if not 'transactionHash' in result and not 'tx_hash' in result: # tx in error
            print("Transaction failed")
            return None
        if not 'transactionHash' in result:
            result['transactionHash'] = result['tx_hash']
        if not 'tx_key' in result:
            result['tx_key'] = "N/A"
        if not 'fee' in result:
            result['fee'] = get_tx_fee(COIN_NAME)
        return result
    return result


async def send_transaction_id(from_address: str, to_address: str, amount: int, paymentid: str, coin: str) -> str:
    coin = coin.upper()
    payload = {
        'addresses': [from_address],
        'transfers': [{
            "amount": amount,
            "address": to_address
        }],
        'fee': get_tx_fee(coin),
        'anonymity': get_mixin(coin),
        'paymentId': paymentid
    }
    result = None
    result = await rpc_client.call_aiohttp_wallet('sendTransaction', coin, payload=payload)
    if result:
        if 'transactionHash' in result:
            return result['transactionHash']
    return result


async def send_transactionall(from_address: str, to_address, coin: str) -> str:
    coin = coin.upper()
    payload = {
        'addresses': [from_address],
        'transfers': to_address,
        'fee': get_tx_fee(coin),
        'anonymity': get_mixin(coin),
    }
    result = None
    result = await rpc_client.call_aiohttp_wallet('sendTransaction', coin, payload=payload)
    if result:
        if 'transactionHash' in result:
            return result['transactionHash']
    return result


async def get_all_balances_all(coin: str) -> Dict[str, Dict]:
    walletCall = await rpc_client.call_aiohttp_wallet('getAddresses', coin)
    wallets = [] ## new array
    for address in walletCall['addresses']:
        wallet = await rpc_client.call_aiohttp_wallet('getBalance', coin, {'address': address})
        wallets.append({'address':address,'unlocked':wallet['availableBalance'],'locked':wallet['lockedAmount']})
    return wallets


async def get_some_balances(wallet_addresses: List[str], coin: str) -> Dict[str, Dict]:
    wallets = []  # new array
    for address in wallet_addresses:
        wallet = await rpc_client.call_aiohttp_wallet('getBalance', coin, {'address': address})
        wallets.append({'address':address,'unlocked':wallet['availableBalance'],'locked':wallet['lockedAmount']})
    return wallets


async def get_sum_balances(coin: str) -> Dict[str, Dict]:
    wallet = None
    result = await rpc_client.call_aiohttp_wallet('getBalance', coin)
    if result:
        wallet = {'unlocked':result['availableBalance'],'locked':result['lockedAmount']}

    return wallet


async def get_balance_address(address: str, coin: str) -> Dict[str, Dict]:
    coin = coin.upper()
    result = await rpc_client.call_aiohttp_wallet('getBalance', coin, {'address': address})
    wallet = None
    if result:
        wallet = {'address':address,'unlocked':result['availableBalance'],'locked':result['lockedAmount']}
    return wallet


async def wallet_optimize_single(subaddress: str, threshold: int, coin: str=None) -> int:
    if coin is None:
        coin = "WRKZ"
    else:
        coin = coin.upper()

    params = {
        "threshold": int(threshold),
        "anonymity": get_mixin(coin),
        "addresses": [
            subaddress
        ],
        "destinationAddress": subaddress
    }
    full_payload = {
        'params': params or {},
        'jsonrpc': '2.0',
        'id': str(uuid4()),
        'method': 'sendFusionTransaction'
    }

    i = 0
    while True:
        #print('get_wallet_api_url(coin): '+ get_wallet_api_url(coin))
        url = get_wallet_api_url(coin) + '/json_rpc'
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=full_payload, timeout=8) as response:
                if response.status == 200:
                    res_data = await response.read()
                    res_data = res_data.decode('utf-8')
                    await session.close()
                    decoded_data = json.loads(res_data)
                    if 'result' in decoded_data:
                        if 'transactionHash' in decoded_data['result']:
                            i=i+1
                        else:
                            break
                    else:
                        break
                else:
                    break
    return i


async def rpc_cn_wallet_save(coin: str):
    coin = coin.upper()
    start = time.time()
    result = await rpc_client.call_aiohttp_wallet('save', coin)
    end = time.time()
    return float(end - start)


async def wallet_estimate_fusion(subaddress: str, threshold: int, coin: str=None) -> int:
    if coin is None:
        coin = "WRKZ"
    else:
        coin = coin.upper()

    payload = {
        "threshold": threshold,
        "addresses": [
            subaddress
        ]
    }
    result = await rpc_client.call_aiohttp_wallet('estimateFusion', coin, payload=payload)
    return result


def get_wallet_api_url(coin: str = None):
    return "http://"+getattr(config,"daemon"+coin,config.daemonWRKZ).wallethost+":"+str(getattr(config,"daemon"+coin,config.daemonWRKZ).walletport)

def get_mixin(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).mixin

def get_decimal(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).decimal

def get_addrlen(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).AddrLen

def get_intaddrlen(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).IntAddrLen

def get_prefix(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).prefix
    
def get_prefix_extra1(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).prefixExtra1
    
def get_prefix_extra2(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).prefixExtra2

def get_prefix_char(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).prefixChar

def get_donate_address(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).DonateAddress

def get_voucher_address(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).voucher_address

def get_diff_target(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).DiffTarget

def get_tx_fee(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).tx_fee

def get_coin_fullname(coin: str = None):
    qr_address_pref = {"TRTL":"turtlecoin","DEGO":"derogold","LCX":"lightchain","CX":"catalyst","WRKZ":"wrkzcoin","OSL":"oscillate",\
    "BTCM":"bitcoinmono","MTIP":"monkeytips","XCY":"cypruscoin","PLE":"plenteum","ELPH":"elphyrecoin","ANX":"aluisyocoin","NBX":"nibbleclassic",\
    "ARMS":"2acoin","HITC":"hitc","NACA":"nashcash","XTOR":"bittoro","BLOG":"blogcoin","LOK":"loki","XTRI":"triton","TRTG":"turtlegold"}
    return getattr(qr_address_pref,coin,"wrkzcoin")


def get_reserved_fee(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).voucher_reserved_fee

def get_min_tx_amount(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).min_tx_amount

def get_max_tx_amount(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).max_tx_amount

def get_interval_opt(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).IntervalOptimize

def get_min_opt(coin: str = None):
    return getattr(config,"daemon"+coin,config.daemonWRKZ).MinToOptimize

def get_coinlogo_path(coin: str = None):
    return config.qrsettings.coin_logo_path + getattr(config,"daemon"+coin,config.daemonWRKZ).voucher_logo

def num_format_coin(amount, coin: str = None):
    if coin is None:
        coin = "WRKZ"
    else:
        coin = coin.upper()
    if coin == "DOGE":
        coin_decimal = 1
    elif coin == "LTC":
        coin_decimal = 1
    else:
        coin_decimal = get_decimal(coin)
    amount_str = 'Invalid.'
    if coin == 	"DOGE":
        return '{:,.6f}'.format(amount)
    if coin_decimal > 1000000:
        amount_str = '{:,.8f}'.format(amount / coin_decimal)
    elif coin_decimal > 10000:
        amount_str = '{:,.6f}'.format(amount / coin_decimal)
    elif coin_decimal > 100:
        amount_str = '{:,.4f}'.format(amount / coin_decimal)
    else:
        amount_str = '{:,.2f}'.format(amount / coin_decimal)
    return amount_str

# XMR
async def validate_address_xmr(address: str, coin: str):
    coin_family = getattr(getattr(config,"daemon"+coin),"coin_family","XMR")
    if coin_family == "XMR":
        payload = {
            "address" : address,
            "any_net_type": True,
            "allow_openalias": True
        }
        address_xmr = await rpc_client.call_aiohttp_wallet('validate_address', coin, payload=payload)
        if address_xmr:
            return address_xmr
        else:
            return None


async def make_integrated_address_xmr(address: str, coin: str, paymentid: str = None):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"daemon"+COIN_NAME),"coin_family","XMR")
    if paymentid:
        try:
            value = int(paymentid, 16)
        except ValueError:
            traceback.print_exc(file=sys.stdout)
            return False
    else:
        paymentid = addressvalidation.paymentid(8)
    if coin_family == "XMR":
        payload = {
            "standard_address" : address,
            "payment_id": {} or paymentid
        }
        address_ia = await rpc_client.call_aiohttp_wallet('make_integrated_address', COIN_NAME, payload=payload)
        if address_ia:
            return address_ia
        else:
            return None


async def get_transfers_xmr(coin: str, height_start: int = None, height_end: int = None):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"daemon"+COIN_NAME),"coin_family","TRTL")
    if height_start is None:
        height_start = 1
    payload = {}
    if coin_family == "XMR":
        payload = {
            "in" : True,
            "out": False,
            "pending": False,
            "failed": False,
            "pool": False,
            "filter_by_height": True,
            "min_height": height_start
        }
        if height_end is not None:
            payload["max_height"] = height_end
        result = await rpc_client.call_aiohttp_wallet('get_transfers', COIN_NAME, payload=payload)
        return result["in"]
    elif coin_family == "TRTL":
        if height_end is None:
            height_end = 1000000000
        payload = {
            "firstBlockIndex" : height_start,
            "blockCount" : height_end-height_start
        }
        result = await rpc_client.call_aiohttp_wallet('getTransactions', COIN_NAME, payload=payload)
        return result["items"]

async def DOGE_LTC_register(account: str, coin: str) -> str:
    payload = f'"{account}"'
    address_call = await rpc_client.call_doge_ltc('getnewaddress', coin.upper(), payload=payload)
    reg_address = {}
    reg_address['address'] = address_call
    payload = f'"{address_call}"'
    key_call = await rpc_client.call_doge_ltc('dumpprivkey', coin.upper(), payload=payload)
    reg_address['privateKey'] = key_call
    return reg_address


async def DOGE_LTC_validaddress(address: str, coin: str) -> str:
    payload = f'"{address}"'
    valid_call = await rpc_client.call_doge_ltc('validateaddress', coin.upper(), payload=payload)
    return valid_call


async def DOGE_LTC_getbalance_acc(account: str, coin: str, confirmation: int=None) -> str:
    if confirmation is None:
        conf = 1
    else:
        conf = confirmation
    payload = f'"{account}", {conf}'
    valid_call = await rpc_client.call_doge_ltc('getbalance', coin.upper(), payload=payload)
    return valid_call


async def DOGE_LTC_getaccountaddress(account: str, coin: str) -> str:
    payload = f'"{account}"'
    valid_call = await rpc_client.call_doge_ltc('getaccountaddress', coin.upper(), payload=payload)
    return valid_call


async def DOGE_LTC_sendtoaddress(to_address: str, amount: float, comment: str, coin: str, comment_to: str=None) -> str:
    if comment_to is None:
        comment_to = "wrkz"
    payload = f'"{to_address}", {amount}, "{comment}", "{comment_to}", true'
    valid_call = await rpc_client.call_doge_ltc('sendtoaddress', coin.upper(), payload=payload)
    return valid_call


async def DOGE_LTC_listreceivedbyaddress(coin: str):
    payload = '0, true'
    valid_call = await rpc_client.call_doge_ltc('listreceivedbyaddress', coin.upper(), payload=payload)
    account_list = []
    if len(valid_call) >=1:
        for item in valid_call:
            account_list.append({"address": item['address'], "account": item['account'], "amount": item['amount']})
    return account_list


async def DOGE_LTC_dumpprivkey(address: str, coin: str) -> str:
    payload = f'"{address}"'
    key_call = await rpc_client.call_doge_ltc('dumpprivkey', coin.upper(), payload=payload)
    return key_call
