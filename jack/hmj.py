"""
Usage:
    okef_bq.py [options]

Options:
    --dry-run
    --play
    --stid=<stid>       stid[default: okef-hmj-eos-usd-bq]
"""

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

from tyz import util


class Extra:
    def __init__(self):
        self.tk1_bbo_place = None
        self.tk2_bbo_place = None

        self.tk1_bbo_dealt = None
        self.tk2_bbo_dealt = None


class Order:
    def __init__(self):
        self.eoid = None
        self.coid = None
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


class Strategy:
    def __init__(self, stid: str):
        self.acc_symbol = ''
        self.cons = []
        self.acc: qbxt.Account

        self.config = Config()
        self.stid = stid
        # symbol=>pos
        self.pos_by_ws = {}
        self.pos_by_rest = {}

        # symbol=>asset
        self.asset_by_ws = {}
        self.asset_by_rest = {}

        self.ticks: Dict[str, qbxt.model.OrderbookUpdate] = {}
        self.bbo: Dict[str, qbxt.model.BboUpdate] = {}

        self.con_basics: Dict[str, qb.Contract] = {}

        self.rc_until = arrow.now()

        self.last_trade_time = 0
        self.inited = False

        self.influxdbudp = util.InfluxdbUdpGauge(self.stid, measurement_prefix=True)

        # coid => Order
        self.active_orders: Dict[str, Order] = {}
        self.abnormal_orders: Dict[str, int] = {}
        # c2_coid => c1_dealt_price
        self.dealt_info: Dict[str, float] = {}

        self.bbo_update_q = asyncio.Queue(1)
        self.last_cancel_all_time = arrow.now()

    @property
    def c1(self):
        return self.cons[0]

    @property
    def c2(self):
        return self.cons[1]

    @property
    def tk1(self) -> qbxt.model.BboUpdate:
        return self.bbo[self.c1]

    @property
    def tk2(self) -> qbxt.model.OrderbookUpdate:
        return self.ticks[self.c2]

    @property
    def pos1(self):
        return self.pos_by_ws[self.c1]['total_amount']

    @property
    def pos2(self):
        return self.pos_by_ws[self.c2]['total_amount']

    @property
    def rc_working(self):
        return arrow.now() < self.rc_until

    def min_change(self, c):
        return self.con_basics[c].min_change

    def min_amount(self, c):
        return self.con_basics[c].min_amount

    @staticmethod
    def rounding(value, unit, func=round):  # copied from qbtrade.util
        fmt = '{:.' + str(len(f'{unit:.10f}'.rstrip('0'))) + 'f}'
        res = unit * func(round(value / unit, 4))
        return float(fmt.format(res))

    @classmethod
    def statsd_tags(cls, tags):
        return ['{}:{}'.format(k, v) for k, v in tags.items()]

    def gauge(self, key: str, value, tags=None, ts=None):
        assert self.stid
        assert tags is None or isinstance(tags, dict)
        # key = 'strategy/{}/{}'.format(self.stid, key)
        if tags is None:
            tags = {}
        if isinstance(value, dict):
            self.influxdbudp.add_point(key, value, tags, ts=ts)
        else:
            self.influxdbudp.add_point(key, {'value': value}, tags, ts=ts)
        # qb.qstatsd.gauge(key, value, tags=self.statsd_tags(tags if tags else {}), interval=interval)

    async def init(self):
        self.config.set_key(f"strategy:{self.stid}:config")
        await self.config.sync()
        self.acc_symbol = self.config.acc
        self.cons = [self.config.c1_symbol, self.config.c2_symbol]

        logging.info(self.c1, self.c2)
        self.quote1 = await qbxt.new_quote('okef', interest_cons=[self.c1],
                                           use_proxy=True,
                                           bbo_callback=self.update_bbo)
        self.quote2 = await qbxt.new_quote('okef', interest_cons=[self.c2],
                                           use_proxy=True,
                                           orderbook_callback=self.update_tick)
        for con in self.cons:
            self.con_basics[con] = qb.con(con)
        await asyncio.sleep(1)
        logging.info('quote okef init')

        # cfg = json.loads(Path('~/.onetoken/okef.ot-mom-1-sub128.json').expanduser().read_text())
        logging.info(f'using account {self.acc_symbol}')
        self.acc = await qbxt.new_account(self.acc_symbol,
                                          # config=cfg,
                                          use_1token_auth=True,
                                          use_proxy=True,
                                          interest_cons=[self.c1, self.c2],
                                          asset_callback=self.asset_callback,
                                          position_callback=self.position_callback,
                                          order_callback=self.order_callback)
        await asyncio.sleep(1)
        logging.info('account okef init')

    async def update_bbo(self, bbo: qbxt.model.BboUpdate):
        if bbo.bid1 is None:
            logging.warning('bid1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        if bbo.ask1 is None:
            logging.warning('ask1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        last_bbo = self.bbo.get(bbo.contract, None)
        if last_bbo:
            now = arrow.now().float_timestamp
            self.gauge('tk1-interval', (now - self.tk1.recv_time / 1e3) * 1e3)
        self.bbo[bbo.contract] = bbo

        now = arrow.now().float_timestamp
        self.gauge('tk1-delay', (now - self.tk1.exg_time / 1e3) * 1e3)

    async def daemon_consume_update_bbo(self):
        while True:
            try:
                await self.bbo_update_q.get()
                await self.main_callback()
            except Exception as e:
                logging.warning('main_callback error', e)

    async def update_tick(self, tk: qbxt.model.OrderbookUpdate):
        if tk.bid1 is None:
            logging.warning('bid1 none', tk.contract, tk.bid1, tk.ask1)
            return
        if tk.ask1 is None:
            logging.warning('ask1 none', tk.contract, tk.bid1, tk.ask1)
            return
        last_tk = self.ticks.get(tk.contract, None)
        if last_tk:
            now = arrow.now().float_timestamp
            self.gauge('tk2-interval', (now - self.tk2.recv_time / 1e3) * 1e3)
        self.ticks[tk.contract] = tk

        now = arrow.now().float_timestamp
        self.gauge('tk2-delay', (now - self.tk2.exg_time / 1e3) * 1e3)
        if now - self.tk2.exg_time / 1e3 > self.config.tick_delay_seconds:
            self.rc_trigger(10, 'tk2-delay')

        if not self.bbo_update_q.empty():  # 清空当前q
            self.bbo_update_q.get_nowait()
        self.bbo_update_q.put_nowait('update_bbo')

    def rc_trigger(self, second, reason):
        if not self.rc_working:
            qb.fut(self.cancel_all())
            logging.warning('rc trigger', reason, second, self.rc_until)
            self.gauge('rc-work', 1, {'type': reason})
        self.rc_until = max(self.rc_until, arrow.now().shift(seconds=second))

    async def rc_work(self):
        if not self.inited:
            self.gauge('rc-work', 1, {'type': 'not-inited'})
            return True
        if self.acc.get_ws_state() != qbxt.const.WSState.READY:
            self.gauge('rc-work', 1, {'type': 'acc-ws'})
            return True
        if self.quote1.get_ws_state() != qbxt.const.WSState.READY:
            self.gauge('rc-work', 1, {'type': 'quote-1-ws'})
            return True
        if self.quote2.get_ws_state() != qbxt.const.WSState.READY:
            self.gauge('rc-work', 1, {'type': 'quote-2-ws'})
            return True
        if not self.config.trade:
            self.gauge('rc-work', 1, {'type': 'trade-off'})
            return True
        if self.rc_working:
            return True

    async def cancel_all(self):
        now = arrow.now()
        if now.shift(seconds=-10) < self.last_cancel_all_time:
            return
        self.last_cancel_all_time = now
        for con in self.cons:
            with Timer('get-pending-list'):
                lst, err = await self.acc.get_pending_list(contract=con)
            if err:
                logging.warning(err)
                return
            for item in lst.orders:
                await asyncio.sleep(0.1)
                with Timer('cancel-order'):
                    res, err = await self.acc.cancel_order(item.exchange_oid)
                if err:
                    logging.warning(err)
                    continue

    def new_coid(self, con, bs):
        return con + '-' + f'{bs}ooooo' + ''.join(random.choices(string.ascii_letters, k=10))

    async def main_callback(self):
        if not self.inited:
            return
        value = {
            'tk1-bid1': self.tk1.bid1,
            'tk1-ask1': self.tk1.ask1,
            'tk2-bid1': self.tk2.bid1,
            'tk2-ask1': self.tk2.ask1,
            'tk-diff': self.tk1.middle / self.tk2.middle,
            'bid-d-ask': self.tk1.bid1 / self.tk2.ask1,
            'ask-d-bid': self.tk1.ask1 / self.tk2.bid1
        }
        self.gauge('quote', value)
        core = math.pow(self.config.diff, -self.pos1 / self.config.amt) * self.config.middle

        core_a = self.tk2.ask1 * core
        core_b = self.tk2.bid1 * core
        # logging.info('c2: ',self.tk2.ask1/self.tk2.bid1)
        # logging.info('core_a/core_b: ',core_a/core_b)
        pb = core_b / math.sqrt(self.config.earn) * self.config.taker_return
        ps = core_a * math.sqrt(self.config.earn) / self.config.taker_return
        # logging.info('ps/pb:',ps,pb, ps/pb)
        tb = pb * self.config.taker_return
        ts = ps / self.config.taker_return
        tb = self.rounding(tb, self.min_change(self.c1), math.floor)
        ts = self.rounding(ts, self.min_change(self.c1), math.ceil)

        # mb = min(self.tk1.aks1- self.min_change(self.c1), pb * self.config.maker_return)
        # ms = max(self.tk1.bid1+ self.min_change(self.c1), ps / self.config.maker_return)
        # logging.info('before rounding', mb,ms, ms/mb)
        mb = pb * self.config.maker_return
        ms = ps / self.config.maker_return
        mb = self.rounding(mb, self.min_change(self.c1), math.floor)
        ms = self.rounding(ms, self.min_change(self.c1), math.ceil)
        # logging.info('after rouding', mb,ms, ms/mb)

        self.gauge('core', core)
        self.gauge('diff-ideal-b', {'maker': mb / self.tk2.bid1, 'taker': tb / self.tk2.bid1})
        self.gauge('diff-ideal-s', {'maker': ms / self.tk2.ask1, 'taker': ts / self.tk2.ask1})
        # pass
        if dryrun:
            return
        if self.pos1 < self.config.amt:
            if tb >= self.tk1.ask1:
                # logging.info(f'try to place taker b {tb}')
                qb.fut(self.do_action('b', tb, self.config.place_amt, False))
            else:
                mb = min(mb, self.tk1.bid1)
                qb.fut(self.do_action('b', mb, self.config.place_amt, self.config.force_maker))
        if -self.config.amt < self.pos1:
            if ts <= self.tk1.bid1:
                # logging.info(f'try to place taker s {ts}')
                qb.fut(self.do_action('s', ts, self.config.place_amt, False))
            else:
                ms = max(ms, self.tk1.ask1)
                qb.fut(self.do_action('s', ms, self.config.place_amt, self.config.force_maker))
        return

    def handle_remain_orders(self, bs, price):
        ideal_order_in_active = False
        for coid, o in list(self.active_orders.items()):
            if bs != o.bs:
                continue
            if price == o.entrust_price and not o.last_cancel_time:
                ideal_order_in_active = True
            else:
                now = arrow.now().float_timestamp
                if not o.first_cancel_time:
                    o.first_cancel_time = now
                factor = self.active_orders[coid].cancel_times + 1
                if not o.last_cancel_time or now - o.last_cancel_time > self.config.cancel_order_interval * factor:
                    if o.eoid:
                        qb.fut(self.cancel_order(eoid=o.eoid))
                    else:
                        qb.fut(self.cancel_order(coid=o.coid))
                    self.active_orders[coid].last_cancel_time = now
                    self.active_orders[coid].cancel_times += 1
                    if self.active_orders[coid].cancel_times > self.config.max_cancel_times:
                        logging.warning(f'{o.coid} {o.eoid} cancel times exceed limit')
                        self.active_orders.pop(o.coid, None)

        return ideal_order_in_active

    async def cancel_order(self, eoid=None, coid=None):
        if eoid:
            with Timer('cancel-order-eoid'):
                res, err = await self.acc.cancel_order(exchange_oid=eoid)
        elif coid:
            with Timer('cancel-order-coid'):
                res, err = await self.acc.cancel_order(client_oid=coid)
        if err:
            if err.code not in ['exg-okef-32004', qbxt.model.Error.EXG_CANCEL_ORDER_NOT_FOUND]:
                logging.warning(res, err, eoid, coid)
            #     if eoid:
            #         qb.fut(self.cancel_order_after_sleep(eoid=eoid, sleep=10))
            #     elif coid:
            #         qb.fut(self.cancel_order_after_sleep(coid=coid, sleep=10))

    async def place_maker_order(self, o: Order):
        action = f'place-maker-order-{o.bs}'
        with Timer(action):
            res, err = await self.acc.place_order(self.c1, price=o.entrust_price, bs=o.bs, amount=o.entrust_amount,
                                                  client_oid=o.coid, options=o.opt)
        if err:
            # 也有可能下单成功
            # del self.active_orders[o.coid]
            self.rc_trigger(self.config.cooldown_seconds, 'place-maker-order')
            return
        if o.coid in self.active_orders.keys():
            self.active_orders[o.coid].eoid = res.exchange_oid

    async def do_action(self, bs: str, price: float, amt: float, force_maker: bool):
        ideal_order_in_active = self.handle_remain_orders(bs, price)
        if ideal_order_in_active:
            return

        if len(self.active_orders) > self.config.max_pending_orders:
            return

        now = arrow.now().float_timestamp
        if now - self.last_trade_time < self.config.trade_interval:
            return
        else:
            self.last_trade_time = now
        if await self.rc_work():
            return

        if bs == 'b':
            amt = min(self.tk2.asks[0][1], self.config.place_amt)
        elif bs == 's':
            amt = min(self.tk2.bids[0][1], self.config.place_amt)

        opt = self.get_options(self.c1, bs, amt, force_maker=force_maker)
        coid = self.new_coid(self.c1, bs)
        o = Order()
        o.coid = coid
        o.entrust_price = price
        o.bs = bs
        o.entrust_amount = amt
        o.entrust_time = arrow.now().float_timestamp
        o.opt = opt
        ext = Extra()
        ext.tk1_bbo_place = self.tk1
        ext.tk2_bbo_place = self.tk2
        o.extra = ext
        self.active_orders[coid] = o
        qb.fut(self.place_maker_order(o))

    async def position_callback(self, pos: qbxt.model.WSPositionUpdate):
        for data in pos.data['positions']:
            self.pos_by_ws[data['contract']] = data
        return

    def gauge_order_extra(self, o: Order):
        field = {
            'tk1-bid1-place': o.extra.tk1_bbo_place.bid1, 'tk1-ask1-place': o.extra.tk1_bbo_place.ask1,
            'tk2-bid1-place': o.extra.tk2_bbo_place.bid1, 'tk2-ask1-place': o.extra.tk2_bbo_place.ask1,
            'tk1-bid1-dealt': self.tk1.bid1, 'tk1-ask1-dealt': self.tk1.ask1,
            'tk2-bid1-dealt': self.tk2.bid1, 'tk2-ask1-dealt': self.tk2.ask1,
        }
        if o.bs == 'b':
            field['diff-b-place'] = o.entrust_price / o.extra.tk2_bbo_place.bid1,
            field['diff-b-dealt'] = o.entrust_price / self.tk2.bid1
        elif o.bs == 's':
            field['diff-s-place'] = o.entrust_price / o.extra.tk2_bbo_place.ask1
            field['diff-s-dealt'] = o.entrust_price / self.tk2.ask1

        if o.first_cancel_time:
            self.gauge('order-cancel-but-dealt', 1, ts=o.first_cancel_time)
        self.gauge('order-extra', field)

    async def order_callback(self, orig: qbxt.model.OrderUpdate):
        order = orig.order
        if order.contract == self.c1:
            # 如果不在程序肯定有问题
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(f'{order.client_oid}, {order.exchange_oid} not in active orders')
                return
            now = arrow.now().float_timestamp
            elapse = now - order_in_memory.entrust_time
            if order_in_memory.first_cancel_time:
                elapse = now - order_in_memory.first_cancel_time
            self.gauge('order-elapse', elapse, tags={'status': order.status})
        if order.status == qbxt.model.Order.PENDING and order.contract == self.c1:
            return

        if 'deal' in order.status and order.contract == self.c1:
            # 如果不在程序肯定有问题
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(f'{order.client_oid}, {order.exchange_oid} not in active orders')
                return
            amt = order.dealt_amount - order_in_memory.dealt_amt
            self.active_orders[order.client_oid].dealt_amt = order.dealt_amount

            self.active_orders[order.client_oid].extra.tk1_bbo_dealt = self.tk1
            self.active_orders[order.client_oid].extra.tk2_bbo_dealt = self.tk2

            if amt > 0:
                bs = 'b' if order.bs == 's' else 's'
                qb.fut(self.place_hedge_order(bs, amt, order.average_dealt_price))

                # c2_price = self.tk2.bid1 if order.bs == 'b' else self.tk2.ask1
                self.gauge('place-price', order.entrust_price, {'bs': order.bs},
                           ts=order_in_memory.entrust_time)
                self.gauge('dealt-amt', amt, tags={'bs': order.bs})
                self.gauge_order_extra(self.active_orders[order.client_oid])
        if order.status == qbxt.model.Order.DEALT and order.contract == self.c2:
            c1_dealt_price = self.dealt_info.get(order.client_oid, None)
            if c1_dealt_price:
                self.gauge('dealt-diff', c1_dealt_price / order.average_dealt_price,
                           tags={'bs': qb.util.op_bs(order.bs)})
                self.dealt_info.pop(order.client_oid, None)

        if 'deal' in order.status:
            con = 'tick1' if order.contract == self.c1 else 'tick2'
            self.gauge('dealt', order.average_dealt_price, tags={'bs': order.bs,
                                                                 'con_symbol': order.contract,
                                                                 'con': con})

        if order.status in qbxt.model.Order.END_SET and order.contract == self.c1:
            self.active_orders.pop(order.client_oid, None)
        return

    async def place_hedge_order(self, bs, amt, c1_dealt_price=None):
        opt = self.get_options(self.c2, bs=bs, amount=amt,
                               force_maker=False)
        coid = self.new_coid(self.c2, bs)
        if c1_dealt_price:
            self.dealt_info[coid] = c1_dealt_price
        if bs == 'b':
            with Timer('place-taker-order'):
                res, err = await self.acc.place_order(self.c2, self.tk2.ask1 * 1.01, bs, amt, client_oid=coid,
                                                      options=opt)
            if err:
                logging.warning(err, f'place-taker-b')
                self.rc_trigger(self.config.cooldown_seconds, 'place-taker')
        elif bs == 's':
            with Timer('place-taker-order'):
                res, err = await self.acc.place_order(self.c2, self.tk2.bid1 * 0.99, bs, amt, client_oid=coid,
                                                      options=opt)
            if err:
                logging.warning(err, f'place-taker-s')
                self.rc_trigger(self.config.cooldown_seconds, 'place-taker')

    async def asset_callback(self, asset: qbxt.model.Assets):
        for data in asset.data['assets']:
            self.asset_by_ws[data['currency']] = data
        return

    async def update_pos(self):
        with Timer('get-positions', {'con': self.c1}):
            pos1, err1 = await self.acc.get_positions(self.c1)
            if not err1:
                for data in pos1.data['positions']:
                    self.pos_by_rest[data['contract']] = data
                try:
                    pos1_gauge = float(self.pos_by_rest[self.c1]['total_amount'])
                except:
                    pos1_gauge = 0
                self.gauge("pos1", pos1_gauge)
            else:
                self.rc_trigger(self.config.cooldown_seconds, 'get-pos1')

        with Timer('get-positions', {'con': self.c2}):
            pos2, err2 = await self.acc.get_positions(self.c2)
            if not err2:
                for data in pos2.data['positions']:
                    self.pos_by_rest[data['contract']] = data
                try:
                    pos2_gauge = float(self.pos_by_rest[self.c2]['total_amount'])
                except:
                    pos2_gauge = 0
                self.gauge("pos2", pos2_gauge)
            else:
                self.rc_trigger(self.config.cooldown_seconds, 'get-pos2')

    async def update_info(self):
        len_active_orders = len(self.active_orders)
        self.gauge('active-orders', len_active_orders)
        if len_active_orders > self.config.max_pending_orders:
            over_due_time = arrow.now().shift(minutes=-30).float_timestamp
            for coid, o in self.active_orders.items():
                if o.entrust_time < over_due_time:
                    logging.warning(f'delete old order {o.bs} , {o.entrust_price}, {o.coid}, {o.eoid}')
                    self.active_orders.pop(coid, None)
        with Timer('get-assets'):
            asset, err = await self.acc.get_assets(contract=self.c1)
        if not err:
            for data in asset.data['assets']:
                self.asset_by_rest[data['currency']] = data
            try:
                total_amount = float(self.asset_by_rest[self.config.coin]['total_amount'])
            except:
                total_amount = 0
            self.gauge('bal', total_amount)
            if self.config.base_amt and self.config.base_amt > 0:
                self.gauge('profit-rate', total_amount / self.config.base_amt)
        else:
            self.rc_trigger(self.config.cooldown_seconds, 'get-assets')
            logging.warning(err)

        await self.update_pos()

    def possum_by_ws(self):
        return self.pos_by_ws[self.c1]['total_amount'] + \
               self.pos_by_ws[self.c2]['total_amount']

    def possum_by_rest(self):
        return self.pos_by_rest[self.c1]['total_amount'] + \
               self.pos_by_rest[self.c2]['total_amount']

    def pos_diff(self):
        score = 0
        if self.possum_by_ws() != 0:
            score += 1
        if self.possum_by_rest() != 0:
            score += 1
        return score >= 1

    async def keep_pos_unchange(self, times, sleep):
        orig = self.possum_by_rest()
        for _ in range(times - 1):
            await asyncio.sleep(sleep)
            await self.update_pos()
            now = self.possum_by_rest()
            if now != orig:
                return False
        return True

    async def match_pos(self):
        is_pos_diff = self.pos_diff()
        if is_pos_diff:
            self.rc_trigger(20,
                            f'pos diff {self.possum_by_rest()}, {self.possum_by_ws()}')
            if await self.keep_pos_unchange(10, 1):
                self.rc_trigger(self.config.cooldown_seconds, 'pos-mismatch')
                await qb.alert_push.push_event(key=f'st-{self.stid}', level=qb.alert_push.ALERTLEVEL.WARNING,
                                               title=f'{self.stid} pos mismatch',
                                               message=f'{self.stid} pos mismatch @minjieh')
                diff_pos = self.possum_by_rest()
                if diff_pos > 0:
                    bs = 's'
                elif diff_pos < 0:
                    bs = 'b'
                diff_pos = abs(diff_pos)
                if diff_pos > self.config.place_amt:
                    diff_pos = self.config.place_amt
                opt = self.get_options(self.c2, bs, diff_pos, False)
                logging.info(f'place mismatch pos order, {bs}, {diff_pos}, {opt}')
                await self.place_hedge_order(bs, diff_pos)

    async def cancel_old_orders(self):
        for con in self.cons:
            with Timer('get-pending-list'):
                lst, err = await self.acc.get_pending_list(contract=con)
            if err:
                logging.warning(err)
                continue
            for item in lst.orders:
                key = f'{item.bs}/{item.entrust_price}'
                if key not in self.active_orders.keys():
                    self.abnormal_orders[item.exchange_oid] = \
                        self.abnormal_orders.get(item.exchange_oid, 0) + 1
                    if self.abnormal_orders[item.exchange_oid] > 2:
                        logging.warning(f'exist abnormal order {item.exchange_oid}, canceling')
                        res, err = await self.acc.cancel_order(item.exchange_oid)
                    if err:
                        logging.warning(err)

    def get_options(self, con, bs, amount, force_maker=False):
        pos = self.pos_by_ws[con]['detail']
        opt = {}
        open_close = 'open'
        if force_maker:
            opt['force_maker'] = True
        if bs == 'b':
            if float(pos['short_avail_qty']) > amount * self.config.open_close_threshold:
                open_close = 'close'
        if bs == 's':
            if float(pos['long_avail_qty']) > amount * self.config.open_close_threshold:
                open_close = 'close'
        opt[open_close] = True
        return opt

    async def init_active_orders(self):
        with Timer('get-pending-list'):
            lst, err = await self.acc.get_pending_list(contract=self.c1)
        if err:
            logging.warning(err)
            return
        for item in lst.orders:
            o = Order()
            o.coid = item.client_oid
            o.eoid = item.exchange_oid
            o.entrust_amount = item.entrust_amount
            o.entrust_price = item.entrust_price
            o.entrust_time = arrow.now().float_timestamp
            o.bs = item.bs
            o.dealt_amt = item.dealt_amount
            self.active_orders[o.coid] = o

    async def run(self):
        await self.init()
        await self.update_info()
        self.asset_by_ws = self.asset_by_rest
        self.pos_by_ws = self.pos_by_rest
        await self.cancel_all()
        await self.config.sync()
        while True:
            await  asyncio.sleep(1)
            if len(self.ticks) + len(self.bbo) != 2:
                logging.warning('waiting tick comes')
                continue
            else:
                break
        self.inited = True
        logging.info("all inited, start...")

        qb.fut(self.daemon_consume_update_bbo())

        qb.fut(qb.autil.loop_call(self.update_info, 30, panic_on_fail=False))
        qb.fut(qb.autil.loop_call(self.config.sync, 30))
        qb.fut(qb.autil.loop_call(self.match_pos, 10))


async def main():
    s = Strategy(stid=stid)
    await s.run()


async def play():
    # s = Strategy(stid)
    # await s.init()
    # await qb.alert_push.push_log(key='minjie', level=qb.alert_push.ALERTLEVEL.WARNING,
    #                              message=f'minjie panic @minjieh')
    s = Strategy(stid)
    # s.gauge('place-price', 2.8, tags={'bs': 'b'})
    s.gauge('dealt-diff', 1, tags={'bs': 's'})


if __name__ == '__main__':
    qb.init(env='prod')
    docopt = docoptinit(__doc__)
    if docopt['--dry-run']:
        dryrun = True
    else:
        dryrun = False
    stid = str(docopt['--stid'])
    logging.info(f'stid: {stid}')
    logging.info(f'dryrun: {dryrun}')
    timerInflux = util.InfluxdbUdpGauge(stid, measurement_prefix=True)
    if docopt['--play']:
        qb.fut(play())
    else:
        qb.fut(main())
    qb.run_forever()
