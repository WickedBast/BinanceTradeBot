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
                        quantity = round(float(binance_client.get_asset_balance('BUSD')['free']) / float(res['p']), 2)
                        order = force(client=binance_client, quantity=quantity, type="BUY")
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
                            order = force(client=binance_client, quantity=quantity, type="SELL")
                            open_position = False
                            selling_price = float(order['fills'][0]['price'])
                            print(f'Order: {order}, Sell Price: {selling_price}')
                            print(f'You made {(selling_price - buy_price) / buy_price}% profit')
                            print(f'It equals to {(selling_price - buy_price) * quantity} dollars')

                            file = open("profits.txt", "a")
                            file.write(f"Profit: {(selling_price - buy_price) / buy_price}% \n")
                            file.write(f'It equals to {(selling_price - buy_price) * quantity} dollars \n')
                            file.close()

                            quantity -= float(order['executedQty'])

            print(f"Holding {quantity} LUNA")


def force(client, quantity, type):
    if type == "BUY":
        try:
            order = client.create_order(
                symbol='LUNABUSD',
                side='BUY',
                type='MARKET',
                quantity=quantity
            )
            return order
        except:
            force(client, (quantity - 0.02), type)
    else:
        try:
            order = client.create_order(
                symbol='LUNABUSD',
                side='SELL',
                type='MARKET',
                quantity=quantity
            )
            return order
        except:
            force(client, (quantity - 0.02), type)


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
