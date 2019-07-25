#!/usr/bin/python3.6
import sys
from config import config
import store
import time
import asyncio

ENABLE_COIN = config.Enable_Coin.split(",")
INTERVAL_EACH = 10


# Let's run balance update by a separate process
async def update_balance():
    print('sleep in second: '+str(INTERVAL_EACH))
    # do not update yet
    for coinItem in ENABLE_COIN:
        asyncio.sleep(INTERVAL_EACH)
        print('Update balance: '+ coinItem.upper().strip())
        start = time.time()
        try:
            await store.sql_update_balances(coinItem.upper().strip())
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        end = time.time()
        print('Done update balance: '+ coinItem.upper().strip()+ ' duration (s): '+str(end - start))

loop = asyncio.get_event_loop()  
loop.run_until_complete(update_balance())  
loop.close()