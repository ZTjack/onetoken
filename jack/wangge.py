"""
Author: Jack
Date: 2020-09-18 14:29:34
LastEditors: Jack
LastEditTime: 2020-09-18 14:34:20
Description:
"""
import _thread as thread
import json
import math
import logging
from collections import defaultdict
import arrow
import hashlib
import gzip
import os
import queue
import hmac
import time
import websocket
import requests
import threading
# import asyncio
from websocket import ABNF
from urllib.parse import urlparse
from operator import itemgetter
import atexit
import sys

# import qbtrade as qb


class AccountWs:
    def __init__(self,
                 symbol: str = None,
                 api_key: str = None,
                 api_secret: str = None):
        """

        :param symbol: exchange/account
        :param api_key: str
        :param api_secret: str
        """
        self.ws = None
        self.pong = 0
        self.symbol = symbol
        self.exchange, self.account = symbol.split('/', 1)
        self.host_ws = 'wss://cdn.1tokentrade.cn/api/v1/ws/trade/{}/{}'.format(
            self.exchange, self.account)
        self.api_key = api_key
        self.api_secret = api_secret
        self.sub_key = set()
        self.handle_info = None
        self.handle_order = None
        self.is_running = False

    @staticmethod
    def gen_sign(secret, verb, path, nonce, data_str):
        """
        签名方法
        :param secret:
        :param verb:
        :param path:
        :param nonce:
        :param data_str:
        :return:
        """
        message = verb + path + str(nonce) + data_str
        print('sign message', message)
        signature = hmac.new(bytes(secret, 'utf8'),
                             bytes(message, 'utf8'),
                             digestmod=hashlib.sha256).hexdigest()
        return signature

    def ws_connect(self):
        # print('Connecting to {}'.format(self.host_ws))
        nonce = str(int(time.time() * 1000000))
        sign = self.gen_sign(self.api_secret,
                             'GET',
                             path='/ws/' + self.account,
                             nonce=nonce,
                             data_str='')
        headers = {
            'Api-Nonce': str(nonce),
            'Api-Key': self.api_key,
            'Api-Signature': sign
        }
        self.ws = websocket.WebSocketApp(self.host_ws,
                                         header=headers,
                                         on_open=self.on_open,
                                         on_data=self.on_data,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.run_forever()

    def send_message(self, message):
        """
        通过 websocket 发送消息
        :param message: string
        :return: None
        """
        self.ws.send(message)

    def send_json(self, js):
        """
        通过 websocket 发送json
        :param js: dict
        :return: None
        """
        self.send_message(json.dumps(js))

    def heart_beat_loop(self):
        while self.ws and self.ws.keep_running:
            try:
                if time.time() - self.pong > 25:
                    logging.warning('connection heart beat lost')
                    break
                else:
                    ping = time.time()
                    self.send_json({'uri': 'ping', 'uuid': ping})
            except Exception as e:
                logging.exception(e)
            time.sleep(10)

    def on_data(self, msg, msg_type, *args):
        try:
            if msg_type == ABNF.OPCODE_BINARY or msg_type == ABNF.OPCODE_TEXT:
                if msg_type == ABNF.OPCODE_TEXT:
                    data = json.loads(msg)
                else:
                    data = json.loads(gzip.decompress(msg).decode())
                uri = data.get('uri')
                if uri == 'pong':
                    self.pong = time.time()
                elif uri in ['connection', 'status']:
                    if data.get('code', data.get('status',
                                                 None)) in ['ok', 'connected']:
                        for key in self.sub_key:
                            self.send_json({'uri': 'sub-{}'.format(key)})
                elif uri == 'info':
                    if data.get('status', 'ok') == 'ok':
                        info = data['data']
                        self.handle_info(info)
                elif uri == 'order':
                    if data.get('status', 'ok') == 'ok':
                        for order in data['data']:
                            self.handle_order(order)
                elif uri in ['sub-order', 'sub-info']:
                    print(data['uri'], data['code'])
                else:
                    print('unhandled', data)
        except Exception as e:
            logging.exception(e)

    def on_open(self):
        self.pong = time.time()
        threading.Thread(target=self.heart_beat_loop).start()

    @staticmethod
    def on_error(ws, error):
        """
        websocket 发生错误的回调
        :param error:
        :return: None
        """
        print('on error', error)
        logging.exception(error)

    @staticmethod
    def on_close(self, *args):
        """
        websocket 关闭的回调
        :return: None
        """
        print("### websocket closed ###")
        self.is_running = False

    def run(self) -> None:
        """
        运行 websocket
        :return: None
        """
        def _run():
            while self.is_running:
                self.ws_connect()

        if self.is_running:
            print('ws is already running')
        else:
            self.is_running = True
            thread.start_new_thread(_run, ())

    def sub_info(self, callback=None):
        def _handle_info(data):
            print('get new info', data)

        self.sub_key.add('info')
        self.handle_info = callback if callback else _handle_info

    def sub_order(self, callback=None):
        def _handle_order(data):
            print('get new order', data)

        self.sub_key.add('order')
        self.handle_order = callback if callback else _handle_order


class Quote:
    def __init__(self, key, ws_url, data_parser):
        # name
        self.key = key
        self.ws_url = ws_url
        self.data_parser = data_parser
        self.ws = None
        self.queue_handlers = defaultdict(list)
        self.data_queue = {}
        self.authorized = False
        self.lock = thread.allocate_lock()
        # for heartbeat
        self.pong = 0
        self.is_running = False

    def ws_connect(self):
        print('Connecting to {}'.format(self.ws_url))
        self.ws = websocket.WebSocketApp(self.ws_url,
                                         on_open=self.on_open,
                                         on_data=self.on_data,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.run_forever()

    def send_message(self, message):
        """
        通过 websocket 发送消息
        :param message: string
        :return: None
        """
        self.ws.send(message)

    def send_json(self, js):
        """
        通过 websocket 发送json
        :param js: dict
        :return: None
        """
        self.ws.send(json.dumps(js))

    def heart_beat_loop(self):
        def run():
            while self.ws and self.ws.keep_running:
                try:
                    if time.time() - self.pong > 20:
                        print('connection heart beat lost')
                        self.ws.close()
                        break
                    else:
                        self.send_json({'uri': 'ping'})
                finally:
                    time.sleep(5)

        thread.start_new_thread(run, ())

    def on_data(self, msg, msg_type, *args):
        try:
            if msg_type == ABNF.OPCODE_BINARY or msg_type == ABNF.OPCODE_TEXT:
                import gzip
                if msg_type == ABNF.OPCODE_TEXT:
                    data = json.loads(msg)
                else:
                    data = json.loads(gzip.decompress(msg).decode())
                uri = data.get('uri', 'data')
                if uri == 'pong':
                    self.pong = arrow.now().timestamp
                elif uri == 'auth':
                    print('auth', data)
                    self.authorized = True
                elif uri == 'subscribe-single-tick-verbose':
                    print('subscribe-single-tick-verbose', data)
                elif uri == 'subscribe-single-zhubi-verbose':
                    print('subscribe-single-zhubi-verbose', data)
                elif uri == 'subscribe-single-candle':
                    print('subscribe-single-candle', data)
                else:
                    q_key, parsed_data = self.data_parser(data)
                    contract = json.loads(q_key)['contract']
                    if contract is None:
                        print('unknown message', data)
                        return
                    if contract in self.data_queue:
                        self.data_queue[contract].put(parsed_data)
        except Exception as e:
            print('msg error...', e)

    def on_open(self):
        def run():
            self.pong = time.time()
            self.heart_beat_loop()
            self.send_json({'uri': 'auth'})
            wait_for_auth = time.time()
            while not self.authorized and time.time() - wait_for_auth < 5:
                time.sleep(0.1)
            if not self.authorized:
                print('wait for auth success timeout')
                self.ws.close()
            q_keys = list(self.queue_handlers.keys())
            if q_keys:
                print('recover subscriptions', q_keys)
                for q_key in q_keys:
                    sub_data = json.loads(q_key)
                    self.subscribe_data(**sub_data)
                    print(sub_data)

        thread.start_new_thread(run, ())

    @staticmethod
    def on_error(error):
        """
        websocket 发生错误的回调
        :param error:
        :return: None
        """
        print('on error', error)

    def on_close(self):
        """
        websocket 关闭的回调
        :return: None
        """
        print('on close')
        self.authorized = False
        print("### websocket closed ###")

    def subscribe_data(self, uri, on_update=None, **kwargs):
        print('subscribe', uri, kwargs)
        while not self.ws or not self.ws.keep_running or not self.authorized:
            time.sleep(1)
        sub_data = {'uri': uri}
        sub_data.update(kwargs)
        q_key = json.loads(json.dumps(sub_data, sort_keys=True))['contract']
        with self.lock:
            try:
                self.send_json(sub_data)
                # print('sub data', sub_data)
                if q_key not in self.data_queue:
                    self.data_queue[q_key] = queue.Queue()
                    if on_update:
                        if not self.queue_handlers[q_key]:
                            self.handle_q(q_key)
            except Exception as e:
                print('subscribe quote failed', str(e))
            else:
                if on_update:
                    self.queue_handlers[q_key].append(on_update)

    def handle_q(self, q_key):
        def run():
            while q_key in self.data_queue:
                q = self.data_queue[q_key]
                try:
                    tk = q.get()
                except Exception as e:
                    print('get data from queue failed', str(e))
                    continue
                for callback in self.queue_handlers[q_key]:
                    try:
                        callback(tk)
                    except Exception as e:
                        logging.exception(e)

        thread.start_new_thread(run, ())

    def run(self) -> None:
        """
        运行 websocket
        :return: None
        """
        def _run():
            while self.is_running:
                self.ws_connect()

        if self.is_running:
            print('ws is already running')
        else:
            self.is_running = True
            thread.start_new_thread(_run, ())

    def close(self):
        """
        关闭 websocket
        :return: None
        """
        self.is_running = False
        self.ws.close()
        self.ws = None  # type: (websocket.WebSocketApp, None)
        self.pong = 0
        self.queue_handlers = defaultdict(list)
        self.data_queue = {}
        self.authorized = False


# 在Quote层级多一级TickV3
class TickV3Quote(Quote):
    def __init__(self):
        super().__init__(
            'tick.v3', 'wss://cdn.1tokentrade.cn/api/v1/ws/tick-v3?gzip=true',
            self.parse_tick)
        self.channel = 'subscribe-single-tick-verbose'
        self.ticks = {}

    def parse_tick(self, data):
        try:
            c = data['c']
            tm = arrow.get(data['tm'])
            et = arrow.get(data['et']) if 'et' in data else None
            tp = data['tp']
            q_key = json.dumps({
                'contract': c,
                'uri': self.channel
            },
                               sort_keys=True)
            if tp == 's':
                bids = [{'price': p, 'volume': v} for p, v in data['b']]
                asks = [{'price': p, 'volume': v} for p, v in data['a']]
                tick = Tick(tm, data['l'], data['v'], bids, asks, c, 'tick.v3',
                            et, data['vc'])
                self.ticks[tick.contract] = tick
                return q_key, tick
            elif tp == 'd':
                if c not in self.ticks:
                    print('update arriving before snapshot' +
                          str(self.channel) + str(data))
                    return None, None
                tick = self.ticks[c].copy()

                tick.time = tm.datetime
                tick.exchange_time = et.datetime
                tick.price = data['l']
                tick.volume = data['v']
                tick.amount = data['vc']
                bids = {p: v for p, v in data['b']}
                old_bids = {
                    item['price']: item['volume']
                    for item in tick.bids
                }
                old_bids.update(bids)
                bids = [{
                    'price': p,
                    'volume': v
                } for p, v in old_bids.items() if v > 0]
                bids = sorted(bids, key=lambda x: x['price'], reverse=True)

                asks = {p: v for p, v in data['a']}
                old_asks = {
                    item['price']: item['volume']
                    for item in tick.asks
                }
                old_asks.update(asks)
                asks = [{
                    'price': p,
                    'volume': v
                } for p, v in old_asks.items() if v > 0]
                asks = sorted(asks, key=lambda x: x['price'])

                tick.bids = bids
                tick.asks = asks
                self.ticks[c] = tick
                return q_key, tick
        except Exception as e:
            print('fail', data, e, type(e))
            logging.exception('parse error')
        return None, None

    def subscribe_tick_v3(self, contract, on_update):
        self.subscribe_data(self.channel,
                            on_update=on_update,
                            contract=contract)


# 存放Tick的数据结构
class Tick:
    def copy(self):
        return Tick(
            time=self.time,
            price=self.price,
            volume=self.volume,
            bids=json.loads(json.dumps(self.bids)),
            asks=json.loads(json.dumps(self.asks)),
            contract=self.contract,
            source=self.source,
            exchange_time=self.exchange_time,
            amount=self.amount,
        )

    def __init__(self,
                 time,
                 price,
                 volume=0,
                 bids=None,
                 asks=None,
                 contract=None,
                 source=None,
                 exchange_time=None,
                 amount=None,
                 **kwargs):

        # internally use python3's datetime
        if isinstance(time, arrow.Arrow):
            time = time.datetime
        assert time.tzinfo
        self.contract = contract
        self.source = source
        self.time = time
        self.price = price
        self.volume = volume
        self.amount = amount
        self.bids = []
        self.asks = []
        if isinstance(time, arrow.Arrow):
            exchange_time = exchange_time.datetime
        if exchange_time:
            assert exchange_time.tzinfo
        self.exchange_time = exchange_time
        if bids:
            self.bids = sorted(bids, key=lambda x: -x['price'])
        if asks:
            self.asks = sorted(asks, key=lambda x: x['price'])
        for item in self.bids:
            assert 'price' in item and 'volume' in item
        for item in self.asks:
            assert 'price' in item and 'volume' in item
            # self.asks = asks

    # last as an candidate of last
    @property
    def last(self):
        return self.price

    @last.setter
    def last(self, value):
        self.price = value

    @property
    def bid1(self):
        if self.bids:
            return self.bids[0]['price']
        return None

    @property
    def ask1(self):
        if self.asks:
            return self.asks[0]['price']
        return None

    @property
    def middle(self):
        if self.ask1 and self.bid1:
            return (self.bid1 + self.ask1) / 2

    def __str__(self):
        return '<{} {}.{:03d} {}/{} {} {} {}>'.format(
            self.contract, self.time.strftime('%H:%M:%S'),
            self.time.microsecond // 1000, self.bid1, self.ask1, self.last,
            self.amount, self.volume)

    def __repr__(self):
        return str(self)


class Config:
    api_key = 'QCsKNH71-n8AqFBer-ZUdnRnKA-y1jsbYhU'
    api_secret = 's53cGW3W-sdEXvOSA-i8JX9oAz-16mMA4sb'
    diff = 1.0020047000551107
    amt = 120
    middle = 1.0107152462366127
    earn = 1.001
    taker_return = 0.99975
    maker_return = 1.00003
    place_amt = 4
    max_pending_orders = 5
    trade_interval = 0.4


class Strategy:
    def __init__(self, contracts, name: str = None, acc_symbol: str = None):
        # config related
        self.name = name  # 策略名
        self.acc_symbol = acc_symbol  # 账户名 okef/jack
        self.contracts = contracts  # 标的
        self.ticks = {}
        self.inited = False  # 初始化完毕标志
        self.active_orders = []  # pending orders
        self.info = {}  # 存放info信息
        self.contract1_setting = {}
        self.contract2_setting = {}
        self.withdrawingOrder = []
        self.last_trade_time = 0

    def on_tick_update(self, tk: Tick):
        # print('new Tick come', tk)
        # delay = (arrow.now() - tk.time).total_seconds()
        if tk.bid1 and tk.ask1:
            if tk.bid1 >= tk.ask1:
                print('bid1 >= ask1 %s %s', tk.bid1, tk.ask1)
        # if delay > 10:
        # print('tick delay second', delay)

    @staticmethod
    def rounding(value, unit, func=round):  # copied from qbtrade.util
        fmt = '{:.' + str(len(f'{unit:.10f}'.rstrip('0'))) + 'f}'
        res = unit * func(round(value / unit, 4))
        return float(fmt.format(res))

    def on_info_update(self, data):
        self.info = data

    def on_order_update(self, data):
        # print('get new order here', data)
        status = None
        if 'status' in data:
            status = data['status']
        if status == 'dealt':
            self.place_hedge_order(data)
            self.active_orders[:] = [
                order for order in self.active_orders
                if order['exchange_oid'] != data['exchange_oid']
            ]
        if status == 'pending':
            self.active_orders.append(data)
        elif status == 'withdrawn':
            self.active_orders[:] = [
                order for order in self.active_orders
                if order['exchange_oid'] != data['exchange_oid']
            ]

    def gen_nonce(self):
        return str(int(time.time() * 1000000))

    def gen_sign(self, secret, verb, endpoint, nonce, data_str):
        # Parse the url so we can remove the base and extract just the path.

        if data_str is None:
            data_str = ''

        parsed_url = urlparse(endpoint)
        path = parsed_url.path

        # print "Computing HMAC: %s" % verb + path + str(nonce) + data
        message = verb + path + str(nonce) + data_str

        signature = hmac.new(bytes(secret, 'utf8'),
                             bytes(message, 'utf8'),
                             digestmod=hashlib.sha256).hexdigest()
        return signature

    def api_call(self,
                 method,
                 endpoint,
                 params=None,
                 data=None,
                 timeout=15,
                 host='https://cdn.1tokentrade.cn/api/v1/trade'):
        assert params is None or isinstance(params, dict)
        assert data is None or isinstance(data, dict)
        method = method.upper()
        nonce = self.gen_nonce()
        url = host + endpoint
        json_str = json.dumps(data) if data else ''
        sign = self.gen_sign(Config.api_secret, method, endpoint, nonce,
                             json_str)
        headers = {
            'Api-Nonce': str(nonce),
            'Api-Key': Config.api_key,
            'Api-Signature': sign,
            'Content-Type': 'application/json'
        }
        res = requests.request(method,
                               url=url,
                               data=json_str,
                               params=params,
                               headers=headers,
                               timeout=timeout)
        return res

    def place_order(
        self,
        contract,
        price,
        bs,
        amount,
    ):
        print('place order', bs, amount, price)
        r = self.api_call('POST',
                          '/{}/orders'.format(self.acc_symbol),
                          data={
                              'contract': contract,
                              'price': price,
                              'bs': bs,
                              'amount': amount
                          })

        result = r.json()
        if 'code' in result:
            error_code = result['code']
            print('place order failed', error_code)
            if error_code == 'place-order-no-money':
                print('No Enough Money')

        if 'exchange_oid' in result:
            print('place order success', result['exchange_oid'])
            # print('cancel order automatically after 10s')
            # t = threading.Timer(5,
            #                     self.cancel_order,
            #                     args=[result['exchange_oid']])
            # t.start()

    def place_hedge_order(self, order):
        average_dealt_price = order['average_dealt_price']
        dealt_amount = order['dealt_amount']
        bs = 'b' if order['bs'] == 's' else 's'
        print('下对冲单')
        self.place_order(self.contract2, average_dealt_price, bs, dealt_amount)

    def get_contract_config(self, contract):
        [exchange, ticker] = contract.split('/')
        r = self.api_call(
            method='GET',
            endpoint='',
            params={
                'exchange': exchange,
                'name': ticker
            },
            host='https://cdn.1tokentrade.cn/api/v1/basic/front/sub-markets')
        return r.json()

    def cancel_order(self, exg_oid):
        if exg_oid in self.withdrawingOrder:
            return
        else:
            self.withdrawingOrder.extend([exg_oid])
        print('撤单', exg_oid)
        r = self.api_call('DELETE',
                          '/{}/orders'.format(self.acc_symbol),
                          params={'exchange_oid': exg_oid})
        result = r.json()[0]
        if 'exchange_oid' in result:
            print('撤单成功', result['exchange_oid'])
            self.withdrawingOrder.remove(result['exchange_oid'])

    def cancel_all(self):
        r = self.api_call('DELETE', '/{}/orders/all'.format(self.acc_symbol))
        print(r.json())

    def get_time(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    @property
    def contract1(self):
        return self.contracts[0]

    @property
    def contract2(self):
        return self.contracts[1]

    @property
    def contract1_tick(self):
        if self.contract1 in self.ticks.data_queue:
            return self.ticks.data_queue[self.contract1].get()
        else:
            return None

    @property
    def contract2_tick(self):
        if self.contract1 in self.ticks.data_queue:
            return self.ticks.data_queue[self.contract2].get()
        else:
            return None

    @property
    def position(self):
        if 'position' in self.info:
            return self.info['position']
        return None

    @property
    def pos1(self):
        if self.position and self.contract1:
            ticker = self.contract1.split('/')[1]
            usdt = [
                coin for coin in self.position if coin['contract'] == ticker
            ]
            if len(usdt):
                return usdt[0]['total_amount']
            else:
                return 0

    @property
    def contract1_min_change(self):
        if 'min_change' in self.contract1_setting:
            return self.contract1_setting['min_change']
        return None

    @property
    def contract2_min_change(self):
        if 'min_change' in self.contract2_setting:
            return self.contract1_setting['min_change']
        return None

    def init(self):
        # 订阅Tick
        self.ticks = TickV3Quote()
        self.ticks.run()
        sub_list = self.contracts
        for contract in sub_list:
            self.ticks.subscribe_tick_v3(contract=contract,
                                         on_update=self.on_tick_update)
        # 订阅position/orders
        self.account = AccountWs(symbol=self.acc_symbol,
                                 api_key=Config.api_key,
                                 api_secret=Config.api_secret)
        self.account.run()
        self.account.sub_info(self.on_info_update)
        self.account.sub_order(self.on_order_update)
        self.contract1_setting = self.get_contract_config(
            self.contract1)['data'][0]['contracts'][0]
        self.contract2_setting = self.get_contract_config(
            self.contract2)['data'][0]['contracts'][0]

    def place_limit_order(self, amount, price, bs):
        if len(self.active_orders) > Config.max_pending_orders:
            print('挂单太多了', len(self.active_orders), Config.max_pending_orders)
            sorted_order = sorted(self.active_orders,
                                  key=itemgetter('entrust_time'))
            oldest_order = sorted_order[0]
            self.cancel_order(oldest_order['exchange_oid'])
            return

        for order in self.active_orders:
            if order['entrust_price'] == price and order['bs'] == bs:
                print('已有相同价格挂单', price, bs)
                return
        now = arrow.now().float_timestamp
        if now - self.last_trade_time < Config.trade_interval:
            print('下单间隔太短')
            return
        else:
            self.last_trade_time = now
            self.place_order(self.contract1, price, bs, amount)

    def check_balance(self):
        while True:
            if 'balance' in self.info:
                if self.info['balance'] < 10000:
                    print('到达最大亏损了，退出程序')
                    os._exit(1)
            time.sleep(10)

    def check_signal(self):
        while True:
            print('%s: Start Loop Tick' % (self.get_time()))
            contract1_tick = self.contract1_tick
            contract2_tick = self.contract2_tick
            pos1 = self.pos1
            contract1_min_change = self.contract1_min_change
            core = math.pow(Config.diff, -pos1 / Config.amt) * Config.middle
            core_a = contract2_tick.ask1 * core
            core_b = contract2_tick.bid1 * core

            pb = core_b / math.sqrt(Config.earn) * Config.taker_return
            ps = core_a / math.sqrt(Config.earn) * Config.taker_return

            tb = pb * Config.taker_return
            ts = ps / Config.taker_return

            tb = self.rounding(tb, contract1_min_change, math.floor)
            ts = self.rounding(ts, contract1_min_change, math.ceil)

            mb = pb * Config.maker_return
            ms = ps / Config.maker_return
            mb = self.rounding(mb, contract1_min_change, math.floor)
            ms = self.rounding(ms, contract1_min_change, math.ceil)

            if pos1 < Config.amt:
                if tb >= contract1_tick.ask1:
                    self.place_limit_order(Config.place_amt, tb, 'b')
                else:
                    mb = min(mb, contract1_tick.bid1)
                    self.place_limit_order(Config.place_amt, mb, 'b')
            if -Config.amt < pos1:
                if ts <= contract1_tick.bid1:
                    self.place_limit_order(Config.place_amt, ts, 's')
                else:
                    ms = max(ms, contract1_tick.ask1)
                    self.place_limit_order(Config.place_amt, ms, 's')
            time.sleep(2)
            # tb = self.rounding(tb, )
            # 限定最大下单量
            # volume = min(tick.volume, 100)
            # if volume:
            #     if tick.bid1 < 0.988:
            #         self.place_limit_order(volume, tick.bid1, 'b')
            #     if tick.ask1 > 0.99:
            #         self.place_limit_order(volume, tick.ask1, 's')
            # time.sleep(2)

    def test(self, a, b):
        print('xxxxxxxx', a + b)
        time.sleep(4)
        sys.exit()

    def exit_handler(self):
        print('My application is ending!')


def main():
    s = Strategy(name='test_strategy',
                 acc_symbol='okef/mock-jack',
                 contracts=['okef/btc.usd.b', 'okef/btc.usd.q'])
    print("start", arrow.now())
    atexit.register(s.exit_handler)
    s.init()
    # 持仓风控
    threading.Thread(target=s.check_balance).start()
    s.check_signal()
    # try:
    #     print("start", arrow.now())
    #     atexit.register(s.exit_handler)
    #     s.init()
    #     # time.sleep(5)
    #     # 持仓风控
    #     threading.Thread(target=s.check_balance).start()
    #     s.check_signal()
    # except Exception as e:
    #     print(e)
    #     s.exit_handler()


if __name__ == '__main__':
    # asyncio.ensure_future(main())
    # asyncio.get_event_loop().run_forever()
    main()
