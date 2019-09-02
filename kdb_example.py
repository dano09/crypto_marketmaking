from cryptofeed.callback import TradeCallback
from cryptofeed import FeedHandler
#from cryptofeed.exchange import bitmex, bitstamp, bitfinex, #GDAX
from cryptofeed.exchange.bitmex import Bitmex
from cryptofeed.exchange.bitstamp import Bitstamp
from cryptofeed.exchange.bitfinex import Bitfinex
from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, BOOK_DELTA

import time
from qpython import qconnection


# In q instance open the port: \p 5002
q = qconnection.QConnection(host='localhost', port=502, pandas=True)
q.open()

q.sendSync("""trades:([]systemtime:`datetime$();
        side:`symbol$();
        amount:`float$();
        price:`float$();
        exch:`symbol$())""")


async def trade(feed, pair, id, timestamp, side, amount, price):
    localtime = time.time()
    print('Feed: {} Pair: {} System Timestamp: {} Amount: {} Price: {} Side: {}'.format(
        feed, pair, localtime, amount, price, side,
    ))
    q.sendSync('`trades insert(.z.z;`{};{};{};`{})'.format(
        side,
        float(amount),
        float(price),
        str(feed)
    ))


def main():
    f = FeedHandler()
    f.add_feed(Bitmex(pairs=['XBTUSD'], channels=[TRADES], callbacks={TRADES: TradeCallback(trade)}))
    f.add_feed(Bitstamp(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES:TradeCallback(trade)}))
    f.add_feed(Bitfinex(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES:TradeCallback(trade)}))
    #f.add_feed(GDAX(pairs=['BTC-USD'], channels=[TRADES], callbacks={TRADES:TradeCallback(trade)}))
    f.run()

if __name__ == '__main__':
    main()