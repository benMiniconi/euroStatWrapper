import asyncio
import websockets
import json
import moment
import Bigquery.WrapperBigQuery as WBQ
import CsvWrapper.QuoteCsvWriter as csvWriter

msg = \
    {"jsonrpc": "2.0",
     "method": "public/ticker",
     "id": 8106,
     "params": {
         "instrument_name": "BTC-PERPETUAL"}
     }

msgforEth = {"jsonrpc": "2.0",
             "method": "public/ticker",
             "id": 8106,
             "params": {
                 "instrument_name": "ETH-PERPETUAL"}
             }
msgSuscribeToBTCUSD = {"jsonrpc": "2.0",
                       "method": "public/subscribe",
                       "id": 42,
                       "params": {
                           "channels": ["ticker.BTC-PERPETUAL.raw"]}
                       }

msgSuscribeToETHUSD = {"jsonrpc": "2.0",
                       "method": "public/subscribe",
                       "id": 42,
                       "params": {
                           "channels": ["ticker.ETH-PERPETUAL.raw"]}
                       }

hearbeat = \
    {
        "jsonrpc": "2.0",
        "id": 9098,
        "method": "public/set_heartbeat",
        "params": {
            "interval": 10
        }
    }

getInstruments = {
    "jsonrpc": "2.0",
    "id": 7617,
    "method": "public/get_instruments",
    "params": {
        "currency": "BTC",
        "kind": "index",
        "expired": False
    }}

websocket = ""
websocket2 = ""
buffer = []


def cleanAssetName(rawAssetName):
    rawAssetNameClean = rawAssetName.replace("-", "_")
    rawAssetNameClean = rawAssetNameClean.replace("PERPETUAL", "USD")
    return rawAssetNameClean.lower()


def prepareJson(rawDeribitJson):
    if "params" in rawDeribitJson.keys():
        data = rawDeribitJson["params"]["data"]
        cleanName = cleanAssetName(data["instrument_name"])
        quote = {"Plateforme": "Deribit", "Asset": cleanName, "Quote": data["index_price"],
                 "Datetime": int(data["timestamp"]) / 1000, "Bid": data["best_bid_price"],
                 "BidAmount": data["best_bid_amount"], "Ask": data["best_ask_price"],
                 "AskAmount": data["best_ask_amount"], "OpenInterest": data["open_interest"]}
        return quote


def manageBuffer(quote):
    if quote:
        buffer.append(quote)
        if len(buffer) >= 300:
            WBQ.writeQuotes(buffer, "deribit")
            emptyBuffer()
            return True
        else:
            return False


def websocketStatus():
    global websocket
    if(isinstance(websocket, websockets.WebSocketClientProtocol)):
        return str(websocket.open)
    else:
        return "Not a websocket yet"


def emptyBuffer():
    global buffer
    buffer = []


async def manageAnswer(wSocket):
    response = await wSocket.recv()
    response_json = json.loads(response)
    csv_file = "crypto" + "Deribit" + moment.now().format("DDMMYYYY") + ".csv"
    quote = prepareJson(response_json)
    if quote: csvWriter.writeQuote(csv_file, [quote])
    manageBuffer(quote)


async def reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD):
    print("in reconnect")
    global websocket
    websocket = ""
    async with websockets.connect('wss://www.deribit.com/ws/api/v2', ping_interval=None) as websocket:
        print("after re-connection", websocket2)
        await websocket.send(msgSuscribeToBTCUSD)
        await websocket.send(msgSuscribeToETHUSD)
        while websocket.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocket)
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)
            except websockets.exceptions.ConnectionClosed:
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)


async def call_api(msgSuscribeToBTCUSD, msgSuscribeToETHUSD):
    global websocket
    websocket = ""
    async with websockets.connect('wss://www.deribit.com/ws/api/v2', ping_interval=None) as websocket:
        print("after connection", websocket)
        await websocket.send(msgSuscribeToBTCUSD)
        await websocket.send(msgSuscribeToETHUSD)
        while websocket.open:
            # await websocket.send(suscribeToBTCUSD)
            try:
                await manageAnswer(websocket)
            except websockets.exceptions.ConnectionClosedOK:
                print("ConnectionClosedOK")
                await websocket.close_connection()
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)
            except websockets.exceptions.ConnectionClosedError:
                print("ConnectionClosedError")
                await websocket.close_connection()
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)
            except websockets.exceptions.ConnectionClosed:
                await websocket.close_connection()
                print("ConnectionClosedOK")
                await asyncio.sleep(60)
                await reconnect(msgSuscribeToBTCUSD, msgSuscribeToETHUSD)

def runWebSocket(loop):
 #asyncio.get_event_loop().run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))
# asyncio.run(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))
    asyncio.set_event_loop(loop)
    loop.run_until_complete(call_api(json.dumps(msgSuscribeToBTCUSD), json.dumps(msgSuscribeToETHUSD)))
