from datetime import timedelta
from alpaca_trade_api import TimeFrame, REST

from .util import (
    daily_cache, parallelize
)

import redis
import pandas as pd

pool = redis.ConnectionPool(host="127.0.0.1",max_connections=50,port=6379,password='muyang1017')
redis_con = redis.StrictRedis(connection_pool=pool)


def list_symbols():
    api = REST()
    return [a.symbol for a in api.list_assets(status="active") if a.tradable]

def get_stockprices(limit=365, timespan='day'):
    all_symbols = list_symbols()

    @daily_cache(filename='alpaca_chart_{}'.format(limit))
    def get_stockprices_cached(all_symbols):
        return _get_stockprices(all_symbols, limit, timespan)

    return get_stockprices_cached(all_symbols)


def _get_stockprices(symbols, limit=365, timespan='day'):
    '''Get stock data (key stats and previous) from Alpaca.
    Just deal with Alpaca's 200 stocks per request limit.
    '''

    api = REST()
    timeframe = TimeFrame.Minute if timespan == "minute" else TimeFrame.Day
    start = None

    #Data v1 used to handle this itself, so we have to do it manually now
    if timespan == 'minute':
        start = api.get_clock().timestamp.date() - timedelta(minutes=limit)
    else:
        start = api.get_clock().timestamp.date() - timedelta(days=limit)

    def fetch(symbols):
        data = {}
        date_str = str(api.get_clock().timestamp.date())
        redis_key = 'bars-'+date_str
        for symbol in symbols:
            df = pd.DataFrame()
            if redis_con.hexists(redis_key,symbol):
                df = pd.read_msgpack(redis_con.hget(redis_key,symbol))
            elif not redis_con.sismember("empty_symbols",symbol):
                if 'USDT' in  symbol or '/USD' in symbol or '/BTC' in symbol:
                    continue
                df = api.get_bars(symbol,start=start,limit=limit,timeframe=timeframe,adjustment='split').df
                if not df.empty:
                    df_msg = df.to_msgpack()
                    result = redis_con.hset(redis_key,symbol,df_msg)
                else:
                    redis_con.sadd("empty_symbols",symbol)
                    print("empty symbol: "+symbol)
                    print(df)

            # Update the index format for comparison with the trading calendar
            if not df.empty:
                if df.index.tzinfo is None:
                    df.index = df.index\
                        .tz_localize('UTC')\
                        .tz_convert('US/Eastern')
                else:
                    df.index = df.index.tz_convert('US/Eastern')


            data[symbol] = df.asfreq('C') if timeframe != 'minute' else df

        return data

    return parallelize(fetch, splitlen=199)(symbols)
