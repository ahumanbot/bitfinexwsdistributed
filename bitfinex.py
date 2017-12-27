import time, datetime, json, thread, websocket
import pymongo
import threading

from pprint import pprint
from bson.objectid import ObjectId

def getTimestamp():
    return int(time.time())    
            
def getDate(timestamp=False):
    timestamp = getTimestamp() if not timestamp else timestamp
    return datetime.datetime.fromtimestamp(timestamp)

class Logger:
    doLogging = False
    def log(self, msg, error=False, status='Usual', curve=True):
        if (error):
            pprint(error)
            
        if (self.doLogging == True):
            if (status == 'Usual'): 
                pprint(msg)
            if (curve): 
                print("--------------------------------------------------")

class Book:
    bids = {}
    asks = {}
    def record(self, price, count, amount):
        book = {
            "price": price,
            #"rate": 0,
            #"period": 0,
            "count": count,
            "amount": amount
        }

        if (amount > 0):
            self.bids[price] = book
            if (count == 0):
                self.bids.pop(price, None)
        else:
            book["amount"] = abs(book["amount"])
            self.asks[price] = book
            if (count == 0):
                self.asks.pop(price, None)

    def proccess(self, d):
        l = list()
        for key in d:
            l.append(d[key])
        return l

    def getAsks(self):
        return self.proccess(self.asks)

    def getBids(self):
        return self.proccess(self.bids)
       
class Trades:
    minuteAmount = 0
    minuteAmountNegative = 0
    minutePrice = 0

    trades = []
    def record(self, trade_id, timestamp, amount, price):
        if amount > 0:
            self.minuteAmount += amount
        if amount < 0:
            self.minuteAmountNegative += amount

        self.minutePrice = price

        self.trades.append([timestamp, amount, price])

    def resetMinuteAmounts(self):
        self.minuteAmount = 0
        self.minuteAmountNegative = 0
        #self.minutePrice = 0

        self.trades = []

class Bitfinex:
    currentVersion = 2

    book = None
    trades = None

    firstTradeReceived = False
    firstBookReceived = False
    
    def __init__(self, fabric):
        self.fabric = fabric        
        
    def log(self, msg, error=False, status='Usual', curve=True):        
        logger.log(msg, error, status, curve)

    def subscribeToBook(self, msg):
        msg = { 
            "event": "subscribe",
            "channel": "book",
            "symbol": "BTCUSD",
            "prec": "P1",
            "freq": "F0",
            "len": 25
        }
        self.ws.send(json.dumps(msg))


    def subscribeToTrades(self, msg):
        msg = {
            "event": "subscribe",
            "channel": "trades",
            "symbol": "BTCUSD"
        }
        self.ws.send(json.dumps(msg))

    def receiveVersion(self, msg):
        version = msg[u'version']
        if (self.currentVersion != version):
            raise ValueError('Bitfinex WS API Version mismatch, current is %d and provided is %d' % (self.currentVersion, version))
        self.log("Bitfinex WS API Version: %d" % self.currentVersion)
        
    def receiveSubscribeStatus(self, msg):
        self.log("Subscribed to channel: %s" % msg[u'channel'])        

    def receiveBook(self, msg):
        for book in msg[1]:
            price, count, amount = book
            self.book.record(price, count, amount)        

    def receiveBookUpdate(self, book):
        self.firstBookReceived = True

        price, count, amount = book[1]
        self.book.record(price, count, amount)

        self.log("Book update: %s %s %s" % (price, count, amount), curve=False, status='Light')
    
    def receiveTrade(self, msg):
        self.firstTradeReceived = True

        data = msg[2]
        trade_id, timestamp, amount, price = data
        self.trades.record(trade_id, timestamp, amount, price)
        self.log("Trade: %s %s %s %s" 
            % (trade_id, timestamp, amount, price), curve=False, status='Light')

    def filterEvent(self, event, channel=None):
        def filter(message):
            if not type(message) is dict: 
                return
            if (not channel):
                return message[u'event'] == event
            else:
                return message[u'event'] == event and message[u'channel'] == channel
        return filter

    def filterTrade(self, message):
        return type(message) is list and message[1] == 'te'

    def filterBookUpdate(self, message):
        return type(message) is list and len(message[1]) == 3

    def proccessChain(self, seq):
        def callback(message):
            commands = seq['commands']        
            for step in range(seq['step'], len(commands)):
                filterFunction = commands[step]['filter']
                if not filterFunction or filterFunction(message):
                    command = commands[step]['call']
                    command(message)

                    if 'aftercall' in commands[step]:
                        commands[step]['aftercall']()
                    return
        return callback

    def on_message(self, ws, message):
        message = json.loads(message)
        return self.proccessChain(self.seq)(message)             
        
    def on_error(self, ws, error):
        self.log(error, error=True)
        self.restart()        

    def on_close(self, ws):
        self.log('Connection closed.')
        self.restart()

    def on_open(self, ws):
        pass

    def restart():
        self.stop()
        self.ws.close()
        time.sleep(5)
        self.start()

    def listenThread(self, *args):
        self.ws.run_forever()

    def collectDataThread(self, *args):   
        lastTime = False
        interval = 60
        synced  = False

        while self.collectData:
            time.sleep(1)
            curTime = getTimestamp()
            
            self.log("%s: Initializing in %s" % (getDate(), interval - curTime % interval))

            if self.firstTradeReceived == False or self.firstBookReceived == False:
                continue

            if curTime % interval == 0:           
                lastTime = curTime    
                break

        self.trades.resetMinuteAmounts()
        self.log("Starting to collect data from %s" % getDate(getTimestamp())) 
        while self.collectData:
            curTime = getTimestamp()
            if curTime % interval == 0 and curTime - interval >= lastTime:
                lastTime = curTime
                
                record = {
                    'start': lastTime,
                    'close': self.trades.minutePrice,
                    'amount': self.trades.minuteAmount,
                    'amountNegative': self.trades.minuteAmountNegative,
                    'trades': self.trades.trades,
                    'asks': self.book.getAsks(),
                    'bids': self.book.getBids()
                }
                self.log(record, curve=False)
                self.log("%s: Record added" % datetime.datetime.fromtimestamp(lastTime))
                collection.insert(record)              
                self.trades.resetMinuteAmounts()

                if not synced:
                    synced = True
                    self.log("Syncing data...")
                    self.fabric.syncClient.sync()

    def start(self):
        self.ws = websocket.WebSocketApp("wss://api.bitfinex.com/ws/2",
                    on_message = self.on_message,
                    on_error = self.on_error,
                    on_close = self.on_close)
        self.ws.on_open = self.on_open

        self.book = Book()
        self.trades = Trades()

        def goToStep(step):
            def callback():
                self.seq['step'] = step
            return callback

        self.seq = {
            'commands': [
                {'filter': None, 'call': self.subscribeToBook, 'aftercall': goToStep(1)},
                {'filter': self.filterEvent(u'info'), 'call': self.receiveVersion, 'aftercall': goToStep(2)},
                {'filter': self.filterEvent(u'subscribed', channel='book'), 'call': self.receiveSubscribeStatus},
                {'filter': None, 'call': self.receiveBook, 'aftercall': goToStep(4)}, 
                {'filter': None, 'call': self.subscribeToTrades, 'aftercall': goToStep(5)},
                {'filter': None, 'call': self.proccessChain({
                    'commands': [
                        {'filter': self.filterEvent(u'subscribed', channel='trades'), 'call': self.receiveSubscribeStatus},
                        {'filter': self.filterTrade, 'call': self.receiveTrade },
                        {'filter': self.filterBookUpdate, 'call': self.receiveBookUpdate }
                    ], 'step': 0
                })},
            ], 'step': 0}

        self.listen = True
        self.collectData = True
        thread.start_new_thread(self.listenThread, ())        
        thread.start_new_thread(self.collectDataThread, ())

    def stop(self):
        self.listen = False
        self.collectData = False


from websocket_server import WebsocketServer
class SyncServer():
    def start(self, port):
        server = WebsocketServer(port)
        server.set_fn_new_client(self.new_client)
        server.set_fn_client_left(self.client_left)
        server.set_fn_message_received(self.message_received)
        thread.start_new_thread(server.run_forever, ())

    def new_client(self, client, server):
        print("New client connected and was given id %d" % client['id'])

    def client_left(self, client, server):
        if(client):
            print("Client(%d) disconnected" % client['id'])

    def message_received(self, client, server, message):
        print("Client(%d) said: %s" % (client['id'], message))
        msg = json.loads(message)
        if (msg['command'] == 'sync'):
            cursor = collection.find( { "start": {"$gt": int(msg['start']), "$lt": int(msg['end'])} } ) 
            for doc in cursor:
                doc['_id'] = None
                server.send_message(client, json.dumps(doc))
        

class SyncClient():
    def __init__(self, port):
        self.ws = websocket.WebSocketApp("ws://localhost:%d" % port,
                    on_message = self.on_message,
                    on_error = self.on_error,
                    on_close = self.on_close)
        self.ws.on_open = self.on_open

    def start(self):
        def run(*args):
            self.ws.run_forever()
        thread.start_new_thread(run, ())

    def sync(self):
        cursor = collection.find({'start': {'$gt': getTimestamp() - 60 * 60 * 24 * 2}}) 
        prevDoc = None
        for doc in cursor:
            if (prevDoc and doc[u'start'] - prevDoc[u'start'] > 60):
                msg = json.dumps({'command': 'sync', 'start': prevDoc[u'start'], 'end': doc[u'start']})
                print("Syncing from: %s,to: %s" % (getDate(prevDoc[u'start']), getDate(doc[u'start'])))
                self.ws.send(msg)
            prevDoc = doc

    def on_open(self, ws):
        print("Connection opened.")

    def on_message(self, ws, message):
        msg = json.loads(message)
        msg['_id'] = ObjectId()
        print("Received record for %s" % getDate(msg[u'start']))
        #collection.update({u'start': msg[u'start']}, msg, True)
        collection.insert(msg)

    def on_error(self, ws, error):
        pprint(error)

    def on_close(self, ws):
        pass

class Fabric:
    def __init__(self):
        pass

    def startBitfinexWs(self):
        self.bitfinex = Bitfinex(self)
        self.bitfinex.start()        
    
    def on_open(self):
        pass

    def startSyncServer(self):
        self.syncServer = SyncServer(port=9000)
        self.syncServer.start()

    def startSyncClient(self):
        self.syncClient = SyncClient(port=9000)
        self.syncClient.start()
        print("Sync client started.")

    def forever(self):
        print("Serving forever.")
        while True:
            pass      
        
    def getSyncAddresses(self, ws):
        pass

    def sync(self, ws):
        pass



if __name__ == "__main__":
    logger = Logger()
    logger.doLogging = True

    websocket.enableTrace(True)
   
    mongo = pymongo.MongoClient("192.168.1.134", 27017, maxPoolSize=1000)
    db = mongo.gekko1
    collection = db[u'bitfinex_real']
    print("Connected to mongo.")
    #collection.drop()  
    collection.create_index(
        [('start', pymongo.ASCENDING)],
        unique=True
    )
    

    wsf = Fabric()
    try:
        #wsf.startSyncServer()

        #time.sleep(1)
        wsf.startSyncClient()

        wsf.startBitfinexWs()

        wsf.forever()        
    except KeyboardInterrupt:
        exit()
