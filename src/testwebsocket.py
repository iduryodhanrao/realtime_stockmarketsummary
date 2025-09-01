#https://pypi.org/project/websocket_client/
import websocket

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=d2q8f59r01qn21mi8cl0d2q8f59r01qn21mi8clg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    #ws.on_open = on_open
    ws.run_forever()
    
    #ws.on_close()