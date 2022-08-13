import requests
from datetime import datetime
from datetime import timedelta
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm
from datetime import timedelta
from tqdm import tqdm
from requests import Session
import traceback
import aiohttp
import asyncio
import itertools

class async_api_get:
    #bybitの銘柄一覧取得
    def bybit_symbols(self):
        while True:
            try:
                api = requests.get(f"https://api.bybit.com/v2/public/tickers").json()
                df = pd.DataFrame(api["result"])
                df["symbol"] = df["symbol"].apply(lambda x: "0" if "21" in x else x)#21期限付き先物の銘柄名を文字列"0"に変える
                df["symbol"] = df["symbol"].apply(lambda x: "0" if "22" in x else x)#22期限付き先物の銘柄名を文字列"0"に変える
                df["symbol"] = df["symbol"].apply(lambda x: "0" if "23" in x else x)#23期限付き先物の銘柄名を文字列"0"に変える
                scli = df["symbol"][df["symbol"] != "0"]    #期限付きじゃないやつだけを抽出
                #print(scli)
                scli_usdt = scli.apply(lambda x: x if "USDT" in x else "0") #文字列USDTが入ってないやつは削除
                list_usdt = list(scli_usdt[scli_usdt != "0"] )
                #print(list_usdt)
                #print(len(list_usdt))
                return list_usdt
            except Exception as e:
                print(e)
                time.sleep(30)

    #非同期処理でapi取得（現先乖離）
    async def dev_only_data(self):
        #https://www.twilio.com/blog/asynchronous-http-requests-in-python-with-aiohttp-jp
        #https://docs.aiohttp.org/en/stable/client_reference.html
        #bybit全銘柄リスト
        all_symbol = self.bybit_symbols()#["ETHUSDT","BTCUSDT"]#
        #何分足にするか
        mins_periods = 60
        #現在時刻
        today = datetime.now()
        #どこまで遡るか
        max_time_delta = int(pd.to_datetime(today).timestamp())-int(pd.to_datetime("2021-01-01").timestamp())##############################ここで自分の好きな日付を入れる################
        #bybitの200期間データ取得を最大何回行えば良いか
        i_max = int(max_time_delta/mins_periods/60/200)
        #全タイムスタンプリスト作成
        timestamp_list = list( map( lambda i: int((pd.to_datetime(today) - i*timedelta(minutes=mins_periods*200)).timestamp()), range(1,i_max) ) )
        #銘柄リスト、タイムスタンプリストの全組み合わせ作成（i番目の0番が銘柄、1番がtimestampのリストが出来る）
        all_list = list( itertools.product(all_symbol,timestamp_list) )
        
        #urlと銘柄、タイムスタンプの合成
        def future_spot_url(i):
            future_url = f"https://api.bybit.com/public/linear/kline?symbol={i[0]}&interval={mins_periods}&from={i[1]}&limit=200"
            spot_url = f"https://api.bybit.com/public/linear/index-price-kline?symbol={i[0]}&interval={mins_periods}&from={i[1]}&limit=200"
            return [future_url,spot_url]
        all_list = map(future_spot_url, all_list)   #all_listのi番目の要素は ( 銘柄, タイムスタンプ )   となっている

        #結果が二重リストになってるのを平坦化する、これで現物も先物も全て合わせた1次元リストになる
        all_list = list( itertools.chain.from_iterable(all_list) )
        
        df = []
        async with aiohttp.ClientSession() as session:
            for url in tqdm(all_list):
                retryt = 10
                while (retryt<=10240):                  #通信エラーリトライ閾値設定
                    try:
                        async with session.get(url) as resp:
                            api = await resp.json()
                            assert resp.status == 200   #通信エラー処理
                            df += api["result"]
                            break
                    except AssertionError as e:
                        traceback.print_exc()
                        time.sleep(retryt)
                        retryt *= 2
                        
        df = pd.DataFrame(df)
        df.rename(columns={"open_time":"date"},inplace=True)
        
        #bybit先物も、現物（インデックス価格）のデータも一緒くたに入ってるから分ける（indexはvolume=NaNだから、そこで区別）
        df_spot = df[["date","symbol","close"]][df["volume"].isnull()].copy()
        #先物はNaNのデータがないから、dropnaすればOK
        df.dropna(inplace=True)
        #先物のうち必要なカラムだけ抜き出し
        df = df[["date","symbol","volume","open","high","low","close"]].copy()
        #最後に先物と現物を結合、現物のcloseは"close_s"とする
        df = pd.merge(df,df_spot,on=["date","symbol"],suffixes=["","_s"])
        print(df)
        return df
    
if __name__ == "__main__":
    aag = async_api_get()
    asyncio.run(aag.dev_only_data())