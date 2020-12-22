import qbtrade as qb
import qbxt
import logging
import arrow
from typing import Dict
import asyncio
import socket
import time
import string
import random
import yaml

class InfluxdbGeneralUDP:
    def __init__(self, host=None, port=None, auto_refresh_interval=0.1):

        if host is None and port is None:
            if qb.config.region == 'awstk':
                host = 'awstk-db-0.machine'
                port = 8089
            else:
                host = 'alihk.influxdb.qbtrade.org'
                port = 8090
        self.host = host
        self.port = port
        self._ls = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.connect((host, port))
        qb.fut(self.auto_flush(auto_refresh_interval))

    def add_point(self, measurement, fields, tags, ts=None):
        """

        :param measurement:
        :param fields:
        :param tags:
        :param ts: time.time() in second
        :return:
        """

        if ts is None:
            ts = time.time()
        ts = int(ts * 1e9)
        line = self.get_line(measurement, fields, tags, ts)
        if line:
            self._ls.append(line)

    @staticmethod
    def get_line(measurement, fields, tags, ts_ns):
        if tags:
            tag_line = ''.join([f',{key}={value}' for key, value in tags.items()])
        else:
            tag_line = ''
        if not fields:
            # print('ignore', fields)
            return ''

        fields_list = []
        for k, v in fields.items():
            if isinstance(v, int):
                fields_list.append(f'{k}={float(v)}')
            if isinstance(v, float):
                fields_list.append(f'{k}={v}')
        if not fields_list:
            return ''
        fields_str = ','.join(fields_list)
        return f'{measurement}{tag_line} {fields_str} {ts_ns}'

    def flush(self):  # udp by design就不是用来做batch的，可用长度太小。
        for line in self._ls:
            self.sock.send(line.encode('utf8'))
        self._ls.clear()

    async def auto_flush(self, interval):
        while True:
            try:
                await asyncio.sleep(interval)
            except RuntimeError:
                print('event loop is closed')
                break
            try:
                a = time.time()
                length = len(self._ls)
                self.flush()
                b = time.time() - a
                if b * 1000 > 3:  # warn if > 3ms rquired
                    ms = round(b * 1000, 2)
                    print(f'long flush use {ms}ms', length)
            except:
                logging.exception('unexpected')
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.sock.connect((self.host, self.port))


influx_udp_client = InfluxdbGeneralUDP()

class InfluxdbUdpGauge:
    def __init__(self, stid, host=None, port=None, measurement_prefix=False):
        if host is None and port is None:
            if qb.config.region == 'awstk':
                host = 'awstk-db-0.machine'
                port = 8089
            else:
                host = 'alihk.influxdb.qbtrade.org'
                port = 8090

        self.host = host
        self.port = port
        self._ls = []
        if measurement_prefix:
            self.measurement = f'strategy/{stid}'
        else:
            self.measurement = f'{stid}'
        # self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.sock.connect((host, port))
        # if auto_flush_interval > 0:
        #     qb.fut(self.auto_flush(auto_flush_interval))

    def add_point(self, key, fields, tags, ts=None):
        """

        :param key:
        :param fields:
        :param tags:
        :param ts:  time.time() in second
        :return:
        """

        influx_udp_client.add_point(self.measurement + '/' + key, fields, tags, ts)

    # def get_line(self, key, fields, tags, ts):
    #     if tags:
    #         tag_line = ''.join([f',{key}={value}' for key, value in tags.items()])
    #     else:
    #         tag_line = ''
    #     if not fields:
    #         # print('ignore', fields)
    #         return ''

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


class Strategy:
    def __init__(self, stid: str):
        self.stid = stid
        self.acc_symbol = 'huobip/subdjw8'
        self.cons = ['huobip/link.usdt', 'huobip/bch.usdt']
        self.bbo: Dict[str, qbxt.model.BboUpdate] = {}
        self.asset_by_ws = {}
        self.asset_by_rest = {}
        self.pos_by_ws = {}
        self.pos_by_rest = {}
        self.active_orders: Dict[str, Order] = {}
        self.dealt_info: Dict[str, float] = {}
        self.bbo_update_q = asyncio.Queue(1)
        self.ticks: Dict[str, qbxt.model.OrderbookUpdate] = {}
        self.con_basics: Dict[str, qb.Contract] = {}
        self.influxdbudp = InfluxdbUdpGauge(self.stid, measurement_prefix=True)

        self.max_pending_orders = 5
        self.base_amt = 300
        self.base_coin = 'usdt'

        self.last_trade_time = 0
        self.trade_interval = 0.4
        self.place_amt = 1
        self.cancel_order_interval = 0.4
        self.max_cancel_times = 5

    @property
    def c1(self):
        return self.cons[0]

    @property
    def c1_coin(self):
        return self.c1.split('/')[1].split('.')[0]

    @property
    def c2(self):
        return self.cons[1]

    @property
    def tk1(self) -> qbxt.model.BboUpdate:
        return self.bbo[self.c1]

    @property
    def tk2(self) -> qbxt.model.OrderbookUpdate:
        return self.ticks[self.c2]

    async def update_bbo(self, bbo: qbxt.model.BboUpdate):
        # {'contract': 'huobip/link.usdt', 'bid1': 13.7752, 'ask1': 13.7803, 'ts': 1605860142928.0, 'exg_time': 1605860142928.0}
        # print('update_bbo > boo', bbo.__dict__)
        if bbo.bid1 is None:
            logging.warning('bid1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        if bbo.ask1 is None:
            logging.warning('ask1 none', bbo.contract, bbo.bid1, bbo.ask1)
            return
        last_bbo = self.bbo.get(bbo.contract, None)
        if last_bbo:
            now = arrow.now().float_timestamp
            # self.gauge('tk1-interval', (now - self.tk1.recv_time / 1e3) * 1e3)
        self.bbo[bbo.contract] = bbo

        # now = arrow.now().float_timestamp
        # self.gauge('tk1-delay', (now - self.tk1.exg_time / 1e3) * 1e3)
    async def asset_callback(self, asset: qbxt.model.Assets):
        # [{'currency': 'usdt', 'total_amount': 300.0, 'available': 300.0, 'frozen': 0.0}]
        # print('asset_callback', asset.data['assets'])
        for data in asset.data['assets']:
            self.asset_by_ws[data['currency']] = data
        return

    async def order_callback(self, orig: qbxt.model.OrderUpdate):
        # {"account": "huobip/subdjw8", "exchange_oid": "huobip/btc.usdt-148405453106144", "client_oid": null, "status": "pending", "contract": "huobip/btc.usdt", "entrust_price": null, "bs": "b", "dealt_amount": 0, "dealt_volume": 0, "entrust_amount": null, "average_dealt_price": null}
        # print('order_callback->order', orig.order)
        order = orig.order
        if order.contract == self.c1:
            # 如果不在程序肯定有问题
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(
                    f'{order.client_oid}, {order.exchange_oid} not in active orders'
                )
                return
            now = arrow.now().float_timestamp
            elapse = now - order_in_memory.entrust_time
            if order_in_memory.first_cancel_time:
                elapse = now - order_in_memory.first_cancel_time
            # self.gauge('order-elapse', elapse, tags={'status': order.status})
        if order.status == qbxt.model.Order.PENDING and order.contract == self.c1:
            return

        if 'deal' in order.status and order.contract == self.c1:
            # 如果不在程序肯定有问题
            order_in_memory = self.active_orders.get(order.client_oid, None)
            if not order_in_memory:
                logging.warning(
                    f'{order.client_oid}, {order.exchange_oid} not in active orders'
                )
                return
            amt = order.dealt_amount - order_in_memory.dealt_amt
            self.active_orders[order.client_oid].dealt_amt = order.dealt_amount

            self.active_orders[order.client_oid].extra.tk1_bbo_dealt = self.tk1
            self.active_orders[order.client_oid].extra.tk2_bbo_dealt = self.tk2

            # if amt > 0:
            #     bs = 'b' if order.bs == 's' else 's'
            # qb.fut(
            #     self.place_hedge_order(bs, amt, order.average_dealt_price))

            # self.gauge('place-price',
            #            order.entrust_price, {'bs': order.bs},
            #            ts=order_in_memory.entrust_time)
            # self.gauge('dealt-amt', amt, tags={'bs': order.bs})
            # self.gauge_order_extra(self.active_orders[order.client_oid])
        if order.status == qbxt.model.Order.DEALT and order.contract == self.c2:
            c1_dealt_price = self.dealt_info.get(order.client_oid, None)
            if c1_dealt_price:
                # self.gauge('dealt-diff',
                #            c1_dealt_price / order.average_dealt_price,
                #            tags={'bs': qb.util.op_bs(order.bs)})
                self.dealt_info.pop(order.client_oid, None)

        if 'deal' in order.status:
            con = 'tick1' if order.contract == self.c1 else 'tick2'
        self.gauge('dealt',
                   order.average_dealt_price,
                   tags={
                       'bs': order.bs,
                       'con_symbol': order.contract,
                       'con': con
                   })

        if order.status in qbxt.model.Order.END_SET and order.contract == self.c1:
            self.active_orders.pop(order.client_oid, None)
        return

    async def position_callback(self, pos: qbxt.model.WSPositionUpdate):
        print('position_callback', pos.data['positions'])
        for data in pos.data['positions']:
            self.pos_by_ws[data['contract']] = data
        return

    async def update_tick(self, tk: qbxt.model.OrderbookUpdate):
        # print('update_tick > bid1', tk.bid1)
        if tk.bid1 is None:
            logging.warning('bid1 none', tk.contract, tk.bid1, tk.ask1)
            return
        if tk.ask1 is None:
            logging.warning('ask1 none', tk.contract, tk.bid1, tk.ask1)
            return
        # last_tk = self.ticks.get(tk.contract, None)
        # if last_tk:
        #     now = arrow.now().float_timestamp
            # self.gauge('tk2-interval', (now - self.tk2.recv_time / 1e3) * 1e3)
        self.ticks[tk.contract] = tk

        # now = arrow.now().float_timestamp
        # self.gauge('tk2-delay', (now - self.tk2.exg_time / 1e3) * 1e3)
        # if now - self.tk2.exg_time / 1e3 > self.config.tick_delay_seconds:
        #     self.rc_trigger(10, 'tk2-delay')

        if not self.bbo_update_q.empty():  # 清空当前q
            self.bbo_update_q.get_nowait()
        self.bbo_update_q.put_nowait('update_bbo')

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

    async def update_info(self):
        len_active_orders = len(self.active_orders)
        if len_active_orders > self.max_pending_orders:
            over_due_time = arrow.now().shift(minutes=-30).float_timestamp
            for coid, o in self.active_orders.items():
                if o.entrust_time < over_due_time:
                    logging.warning(f'delete old order {o.bs} , {o.entrust_price}, {o.coid}, {o.eoid}')
                    self.active_orders.pop(coid, None)
        asset, err = await self.acc.get_assets(contract=self.c1)
        if not err:
            for data in asset.data['assets']:
                self.asset_by_rest[data['currency']] = data
            try:
                total_amount = float(self.asset_by_rest[self.base_coin]['total_amount'])
            except:
                total_amount = 0
            self.gauge('position', total_amount)
            if self.base_amt and self.base_amt > 0:
                self.gauge('profit-rate', total_amount / self.base_amt)
        else:
            # self.rc_trigger(self.config.cooldown_seconds, 'get-assets')
            logging.warning(err)

    def new_coid(self, con, bs):
        return con + '-' + f'{bs}ooooo' + ''.join(random.choices(string.ascii_letters, k=10))

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
                if not o.last_cancel_time or now - o.last_cancel_time > self.cancel_order_interval * factor:
                    if o.eoid:
                        qb.fut(self.cancel_order(eoid=o.eoid))
                    else:
                        qb.fut(self.cancel_order(coid=o.coid))
                    self.active_orders[coid].last_cancel_time = now
                    self.active_orders[coid].cancel_times += 1
                    if self.active_orders[coid].cancel_times > self.max_cancel_times:
                        logging.warning(f'{o.coid} {o.eoid} cancel times exceed limit')
                        self.active_orders.pop(o.coid, None)

        return ideal_order_in_active

    async def cancel_order(self, eoid=None, coid=None):
        if eoid:
            res, err = await self.acc.cancel_order(exchange_oid=eoid)
        elif coid:
            res, err = await self.acc.cancel_order(client_oid=coid)
        if err:
            if err.code not in ['exg-okef-32004', qbxt.model.Error.EXG_CANCEL_ORDER_NOT_FOUND]:
                logging.warning(res, err, eoid, coid)
            #     if eoid:
            #         qb.fut(self.cancel_order_after_sleep(eoid=eoid, sleep=10))
            #     elif coid:
            #         qb.fut(self.cancel_order_after_sleep(coid=coid, sleep=10))

    async def do_action(self, bs: str, price: float, amt: float, force_maker: bool):
        ideal_order_in_active = self.handle_remain_orders(bs, price)
        if ideal_order_in_active:
            return

        if len(self.active_orders) > self.max_pending_orders:
            return

        now = arrow.now().float_timestamp
        if now - self.last_trade_time < self.trade_interval:
            return
        else:
            self.last_trade_time = now
        # if await self.rc_work():
        #     return

        if bs == 'b':
            # amt = min(self.tk1.asks[0][1], self.place_amt)
            amt = self.place_amt
        elif bs == 's':
            amt = self.place_amt
            # amt = min(self.tk1.bids[0][1], self.place_amt)

        coid = self.new_coid(self.c1, bs)
        o = Order()
        o.coid = coid
        o.entrust_price = price
        o.bs = bs
        o.entrust_amount = amt
        o.entrust_time = arrow.now().float_timestamp
        self.active_orders[coid] = o
        qb.fut(self.place_maker_order(o))

    async def place_maker_order(self, o: Order):
        res, err = await self.acc.place_order(self.c1, price=o.entrust_price, bs=o.bs, amount=o.entrust_amount,
                                   client_oid=o.coid, options=o.opt)
        if err:
            # 也有可能下单成功
            # del self.active_orders[o.coid]
            # self.rc_trigger(self.config.cooldown_seconds, 'place-maker-order')
            return
        if o.coid in self.active_orders.keys():
            self.active_orders[o.coid].eoid = res.exchange_oid

    async def get_config_dict(self):
        print('get redis settings')
        conn = await qb.util.get_async_redis_conn()
        yml = await conn.get('strategy:okef-hmj-btc-usd-bq:config.yml')
        if yml:
            print(yaml.load(yml))
            return yaml.load(yml)
        else:
            qb.panic('no config found')

    async def init(self):
        # 行情
        self.quote1 = await qbxt.new_quote('huobip',
                                           interest_cons=[self.c1],
                                           use_proxy=True,
                                           bbo_callback=self.update_bbo)
        self.quote2 = await qbxt.new_quote('huobip',
                                           interest_cons=[self.c2],
                                           use_proxy=True,
                                           orderbook_callback=self.update_tick)
        for con in self.cons:
            self.con_basics[con] = qb.con(con)
        await asyncio.sleep(1)
        logging.info('quote huobip init')

        # cfg = json.loads(Path('~/.onetoken/okef.ot-mom-1-sub128.json').expanduser().read_text())
        logging.info(f'using account {self.acc_symbol}')
        self.acc = await qbxt.new_account(
            self.acc_symbol,
            # config=cfg,
            use_1token_auth=True,
            use_proxy=True,
            interest_cons=[self.c1, self.c2],
            asset_callback=self.asset_callback,
            position_callback=self.position_callback,
            order_callback=self.order_callback)
        await asyncio.sleep(1)
        logging.info('account huobip init')


async def main():
    stid = 'st-jack-qbxt-demo'
    s = Strategy(stid=stid)
    await s.get_config_dict()

    # await s.init()
    # await s.update_info()
    # qb.fut(qb.autil.loop_call(s.update_info, 30, panic_on_fail=False))
    # qb.fut(s.do_action('b', 12, s.place_amt, False))


if __name__ == '__main__':
    qb.fut(main())
    qb.run_forever()
