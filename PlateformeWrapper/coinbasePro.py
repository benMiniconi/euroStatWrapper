# # btc_heartbeat.py
import asyncio
import websockets
import json
import moment
import datetime
import CsvWrapper.QuoteCsvWriter as csvWriter
from Bigquery import WrapperBigQuery as WBQ

websocketCoin = ""
bufferCoinbase = []

msg = {
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "ETH-EUR",
        "BTC-USD",
        "BTC-EUR"
    ],
    "channels": ["ticker"]
}


def manageBuffer(quote):
    if quote:
        bufferCoinbase.append(quote)
        if len(bufferCoinbase) >= 300:
            WBQ.writeQuotes(bufferCoinbase, "coinbase")
            emptyBuffer()
            return True
        else:
            return False


def websocketStatus():
    global websocketCoin
    if (isinstance(websocketCoin, websockets.WebSocketClientProtocol)):
        return str(websocketCoin.open)
    else:
        return "Not a websocket yet"


def emptyBuffer():
    global bufferCoinbase
    bufferCoinbase = []


def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("-", "_")
    rawAssetNameClean = rawAssetNameClean.replace("XBT", "btc")
    return rawAssetNameClean.lower()


def prepareJson(rawDeribitJson):
    if type(rawDeribitJson) == dict and "price" in rawDeribitJson.keys():
        data = rawDeribitJson
        asset = cleanAssetName(data['product_id'])
        timesta = datetime.datetime.now().timestamp()
        quote = {"Plateforme": "Coinbase", "Asset": asset, "Quote": float(data["price"]), "Datetime": timesta,
                 "Bid": float(data["best_bid"]), "BidAmount": 0, "Ask": float(data["best_ask"]), "AskAmount": 0,
                 "OpenInterest": float(data["open_24h"])}
        return quote


async def manageAnswer(Wsocket):
    response = await Wsocket.recv()
    response_json = json.loads(response)
    # response_json
    csv_file = "crypto" + "Coinbase" + moment.now().format("DDMMYYYY") + ".csv"
    quote = prepareJson(response_json)
    if quote: csvWriter.writeQuote(csv_file, [quote])
    manageBuffer(quote)


async def reconnect(msg):
    async with websockets.connect('wss://ws-feed.pro.coinbase.com') as websocketCoin:
        print(msg)
        await websocketCoin.send(msg)

        while websocketCoin.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocketCoin)
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await call_api(msg)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await asyncio.sleep(60)
                await call_api(msg)
            except websockets.exceptions.ConnectionClosed:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await call_api(msg)


async def call_api(msg):
    async with websockets.connect('wss://ws-feed.pro.coinbase.com') as websocketCoin:
        print(msg)
        await websocketCoin.send(msg)

        while websocketCoin.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocketCoin)
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msg)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await asyncio.sleep(60)
                await reconnect(msg)
            except websockets.exceptions.ConnectionClosed:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msg)


# asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msg)))

def runWebSocket(loop):
    # asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))
    # asyncio.run(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))
    asyncio.set_event_loop(loop)
    loop.run_until_complete(call_api(json.dumps(msg)))
