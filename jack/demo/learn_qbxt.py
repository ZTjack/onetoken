"""
Usage:
    okef_bq.py [options]

Options:
    --dry-run
    --play
    --stid=<stid>       stid[default: okef-hmj-eos-usd-bq]
"""

import qbtrade as qb
import qbxt
import logging
import arrow
import asyncio


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
        self.acc_symbol = 'okef/mock-jack'
        self.cons = ['okef/btc.usd.2021-03-26', 'okef/btc.usd.2020-12-25']
        self.bbo: Dict[str, qbxt.model.BboUpdate] = {}
        self.asset_by_ws = {}
        self.pos_by_ws = {}
        self.active_orders: Dict[str, Order] = {}
        self.dealt_info: Dict[str, float] = {}

    @property
    def c1(self):
        return self.cons[0]

    @property
    def c2(self):
        return self.cons[1]

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
            # self.gauge('tk1-interval', (now - self.tk1.recv_time / 1e3) * 1e3)
        self.bbo[bbo.contract] = bbo

        # now = arrow.now().float_timestamp
        # self.gauge('tk1-delay', (now - self.tk1.exg_time / 1e3) * 1e3)        
    async def asset_callback(self, asset: qbxt.model.Assets):
        for data in asset.data['assets']:
            self.asset_by_ws[data['currency']] = data
        return

    async def order_callback(self, orig: qbxt.model.OrderUpdate):
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

        # if 'deal' in order.status:
        #     con = 'tick1' if order.contract == self.c1 else 'tick2'
            # self.gauge('dealt',
            #            order.average_dealt_price,
            #            tags={
            #                'bs': order.bs,
            #                'con_symbol': order.contract,
            #                'con': con
            #            })

        if order.status in qbxt.model.Order.END_SET and order.contract == self.c1:
            self.active_orders.pop(order.client_oid, None)
        return

    async def position_callback(self, pos: qbxt.model.WSPositionUpdate):
        for data in pos.data['positions']:
            self.pos_by_ws[data['contract']] = data
        return

    def init(self):
        self.quote1 = await qbxt.new_quote('okef',
                                           interest_cons=[self.c1],
                                           use_proxy=True,
                                           bbo_callback=self.update_bbo)
        self.quote2 = await qbxt.new_quote('okef',
                                           interest_cons=[self.c2],
                                           use_proxy=True,
                                           orderbook_callback=self.update_tick)
        for con in self.cons:
            self.con_basics[con] = qb.con(con)
        await asyncio.sleep(1)
        logging.info('quote okef init')

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
        logging.info('account okef init')


async def main():
    s = Strategy(stid=stid)
    await s.init()



if __name__ == '__main__':
    qb.fut(play())
    qb.run_forever()
