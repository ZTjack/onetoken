import qbtrade as qb
import qbxt
import logging
import arrow
from typing import Dict
import asyncio

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
        self.pos_by_ws = {}
        self.active_orders: Dict[str, Order] = {}
        self.dealt_info: Dict[str, float] = {}
        self.bbo_update_q = asyncio.Queue(1)
        self.ticks: Dict[str, qbxt.model.OrderbookUpdate] = {}
        self.con_basics: Dict[str, qb.Contract] = {}
        self.influxdbudp = InfluxdbUdpGauge(self.stid, measurement_prefix=True)

    @property
    def c1(self):
        return self.cons[0]

    @property
    def c2(self):
        return self.cons[1]

    async def update_bbo(self, bbo: qbxt.model.BboUpdate):
        # {'contract': 'okef/btc.usd.2021-03-26', 'bid1': 17166.3, 'ask1': 17167.08, 'exg_time': 1605607915822.0}
        # print('update_bbo > boo', bbo.bbo)
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
        self.gauge('order-update', orig.order)
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

    async def init(self):
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
    await s.init()


if __name__ == '__main__':
    qb.fut(main())
    qb.run_forever()
