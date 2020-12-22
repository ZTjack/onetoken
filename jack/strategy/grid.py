import asyncio
import logging
import math
import random
import string
from typing import Dict

import arrow
import qbtrade as qb
import qbxt
import yaml
from docopt import docopt as docoptinit

class Order:
    def __init__(self):
        self.eoid = None # exchange_id
        self.coid = None # client_id
        self.status = None
        self.bs = None
        self.entrust_time = None  # 本地下单的arrow.now().float_timestamp
        self.entrust_price = None
        self.entrust_amount = None
        self.dealt_amt = 0
        self.first_cancel_time = None
        self.last_cancel_time = None
        self.opt = {}
        self.cancel_times = 0
        self.extra = None

class Config:
    def __init__(self):
        self.price_rule = 'market'  # limit: bid被bid1约束；market: bid被ask1约束；taker: 无视盘口。
        self.maker_return = 1.0  # 作为maker的回报率。当手续费为0.1%，回报率为0.999；当享受0.05% rebate时，回报率1.0005。
        self.taker_return = 1.0  # 同上
        self.level_count = 3  # 挂出去3档
        self.amt = None  # 最多持有仓位
        self.diff = None  # e.g. 1.01 整个diff 由 diff 定
        self.earn = None  # e.g. 1.005
        self.trade = True
        self.force_maker = True
        self.tick_delay_seconds = 3
        self.trade_interval = 3

        self.stable_seconds = 3  # do action 需要持续x秒才真的do action

        self.middle = 1
        self.place_amt = 1

        self.acc = ''
        self.coin = ''
        self.c1_symbol = ''
        self.c2_symbol = ''
        self.cooldown_seconds = 60  # 60s
        self.max_pending_orders = 2
        self.open_close_threshold = 5
        self.cancel_order_interval = 1
        self.max_cancel_times = 5
        self.base_amt = None

    def set_key(self, k):
        self.key = k

    async def get_config_dict(self):
        conn = await qb.util.get_async_redis_conn()
        yml = await conn.get(self.key + '.yml')
        if yml:
            return yaml.load(yml)
        else:
            qb.panic(f'no config found {self.key}')

    async def sync(self, display=False):
        dct = await self.get_config_dict()
        if display:
            logging.info('load', dct)
        self.load_dict(dct)

    def load_dict(self, dct):
        if not dct:
            return
        for k, v in dct.items():
            orig = getattr(self, k)
            if orig != v:
                logging.info('config change', k, v)
            setattr(self, k, v)

# 工具类
class Timer(object):
    def __init__(self, action, tags=None):
        if tags:
            self.tags = tags
        else:
            self.tags = {}
        self.tags['action'] = action

    def __enter__(self):
        self.start = arrow.now()

    def __exit__(self, *args, **kwargs):
        self.end = arrow.now()
        key = 'elapse'
        diff = (self.end - self.start).total_seconds()
        timerInflux.add_point(key=key, tags=self.tags, fields={'value': diff})

async def main():
    print('main')
    # s = Strategy(stid=stid)
    # await s.run()


async def play():
    print('play demo')


if __name__ == '__main__':
    qb.init(env='prod')
    docopt = docoptinit(__doc__)
    stid = str(docopt['--stid'])
    logging.info(f'stid: {stid}')
    if docopt['--play']:
        qb.fut(play())
    else:
        qb.fut(main())
    qb.run_forever()