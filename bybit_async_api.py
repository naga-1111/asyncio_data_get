import asyncio
import aiohttp
import async_timeout
from aiohttp import ClientError
import datetime
import itertools
import requests
import time
import pandas as pd

class bybit_data_manage:
  """
  https://gist.github.com/rhoboro/86629f831934827d832841709abfe715
  """
  async def _fetch(self, session, url, coro):
      """HTTPリソースからデータを取得しコルーチンを呼び出す
      :param session: aiohttp.ClientSessionインスタンス
      :param url: アクセス先のURL
      :param coro: urlとaiohttp.ClientResponseを引数に取るコルーチン
      :return: coroの戻り値
      """
      async with async_timeout.timeout(10):
        try:
            response = await session.get(url)
        except ClientError as e:
            print(e)
            response = None
      return await coro(url, response)


  async def _bound_fetch(self, semaphore, url, session, coro):
      """並列処理数を制限しながらHTTPリソースを取得するコルーチン
      :param semaphore: 並列数を制御するためのSemaphore
      :param session: aiohttp.ClientSessionインスタンス
      :param url: アクセス先のURL
      :param coro: urlとaiohttp.ClientResponseを引数に取るコルーチン
      :return: coroの戻り値
      """
      async with semaphore:
          return await self._fetch(session, url, coro)


  async def _run(self, urls, coro, limit=1):
      """並列処理数を制限しながらHTTPリソースを取得するコルーチン
      :param urls: URLの一覧
      :param coro: urlとaiohttp.ClientResponseを引数に取るコルーチン
      :param limit: 並列実行の最大数
      :return: coroの戻り値のリスト。urlsと同順で返す
      """
      tasks = []
      semaphore = asyncio.Semaphore(limit)
      # [SSL: CERTIFICATE_VERIFY_FAILED]エラーを回避する
      async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
          for url in urls:
              task = asyncio.ensure_future(self._bound_fetch(semaphore, url, session, coro))
              tasks.append(task)
          responses = await asyncio.gather(*tasks)
          return responses


  async def coroutine(self, url, response):
    """url, responseを受け取る任意のコルーチン
    """
    #return url, response.status, await response.json()
    return await response.json()


  def make_event_loop(self, urls, coro, limit=3):
    """並列処理数を制限しながらHTTPリソースを取得し、任意の処理を行う
    :param urls: URLの一覧
    :param coro: urlとaiohttp.ClientResponseを引数に取る任意のコルーチン
    :param limit: 並列実行の最大数
    :return: coroの戻り値のリスト。urlsと同順。
    """
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(self._run(urls, coro, limit))
    return results


  def async_main(self, urls):
    """
    results = self.make_event_loop(urls=urls, coro=self.coroutine, limit=3)
    for url, status, body in results:
        print(url, status, body)
    """
    results = self.make_event_loop(urls=urls, coro=self.coroutine, limit=10)
    return results


  def bybit_symbols(self):
      """
      全銘柄リスト作成
      """
      retryt = 10
      while (retryt <= 40):
          try:
              response = requests.get(f"https://api.bybit.com/derivatives/v3/public/tickers?category=linear")
              if response.status_code != 200:
                  raise TestError
              response = response.json()
              symbols = [ i["symbol"] for i in response["result"]["list"] ]
              return symbols
          except TestError:
              time.sleep(self.retryt)
              retryt *= 2

  def bybit_index_future_list(self):
    now_t = datetime.datetime.now()                             # 現在時刻
    startt = int(int(now_t.timestamp()) - 60*60*19)             # 開始時間

    def url_str_list(symbol:str) -> list:
      """future,indexのohlcv取得用urlリスト作成
      Args:
        symbol:str
      """
      future = f"https://api.bybit.com/public/linear/kline?symbol={symbol}&interval=60&from={startt}&limit=20"
      index = f"https://api.bybit.com/public/linear/index-price-kline?symbol={symbol}&interval=60&from={startt}&limit=20"
      return [future, index]

    url_list = [url_str_list(symbol) for symbol in self.bybit_symbols() ]     #テスト["BTCUSDT","ETHUSDT","BNBUSDT"]
    url_list = itertools.chain.from_iterable(url_list)                        # 3次元配列を平滑化
    results = self.async_main(url_list)
    print(results)
    return

class TestError(Exception):
  pass

if __name__ == '__main__':
  bdm = bybit_data_manage()
  bdm.bybit_index_future_list()
