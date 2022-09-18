import asyncio
import os

import pandas as pd
import ta
from binance import AsyncClient, BinanceSocketManager
from binance.client import Client
from dotenv import load_dotenv


async def main():
    load_dotenv()
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.trade_socket('LUNABUSD')

    binance_client = Client(api_key=os.getenv('BINANCE_API_KEY'), api_secret=os.getenv('BINANCE_SECRET_KEY'))
    df = pd.DataFrame()
    open_position = False
    quantity = 0

    async with ts as tscm:
        while True:
            res = await tscm.recv()
            df = df.append(createFrame(res))
            if len(df) > 30:
                if not open_position:
                    if ta.momentum.roc(df.Price, 30).iloc[-1] > 0 and \
                            ta.momentum.roc(df.Price, 30).iloc[-2]:
                        order = binance_client.create_order(
                            symbol='LUNABUSD',
                            side='BUY',
                            type='MARKET',
                            quantity=round(
                                float(binance_client.get_asset_balance('BUSD')['free']) / float(res['p']), 2
                            ) - 0.2
                        )
                        open_position = True
                        buy_price = float(order['fills'][0]['price'])
                        print(f'Order: {order}, Buy Price: {buy_price}')

                if open_position:
                    subdf = df[df.Time >= pd.to_datetime(order['transactTime'], unit='ms')]

                    if len(subdf) > 1:
                        subdf['highest'] = subdf.Price.cummax()
                        subdf['trailingstop'] = subdf['highest'] * 0.995
                        if subdf.iloc[-1].Price < subdf.iloc[-1].trailingstop or \
                                df.iloc[-1].Price / float(order['fills'][0]['price']) > 1.002:
                            order = binance_client.create_order(
                                symbol='LUNABUSD',
                                side='SELL',
                                type='MARKET',
                                quantity=quantity - 0.1
                            )
                            open_position = False
                            selling_price = float(order['fills'][0]['price'])
                            print(f'Order: {order}, Sell Price: {selling_price}')
                            print(f'You made {(selling_price - buy_price) / buy_price} profit')
                            print(f'It equals to {(selling_price - buy_price) * quantity} dollars')

                            file = open("profits.txt", "a")
                            file.write(f"Profit: {(selling_price - buy_price) / buy_price}")
                            file.close()
            print(open_position)
            print(quantity)


def createFrame(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['Symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
