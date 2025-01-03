import asyncio
from stratagy_maintain import StrategyMaintainer
from socket_setup import WebSocketManager
from concurrent.futures import ThreadPoolExecutor
import time
from SmartApi import SmartConnect
import pyotp
import os

def create_session():
        """
        Creates a session using Smart API.
        """
        print("WebSocketManager: create_session method invoked")
        try:
            smart_api = SmartConnect(api_key=os.getenv('apikey'))
            secret = os.getenv('see')
            totp = pyotp.TOTP(secret)
            token = totp.now()
            session = smart_api.generateSession(os.getenv('client'), os.getenv('passc'), token)
            return session['data'], smart_api
        except Exception as e:
            print(f"Error creating session: {e}")
            return None, None
        
async def main():

    session_data, smart_api = create_session()

    executor = ThreadPoolExecutor(max_workers=4) 

    websocket_manager = WebSocketManager(strategy_maintainer=None)
    executor.submit(asyncio.run, websocket_manager.setup_websocket(session_data=session_data)) 

    strategy_maintainer = StrategyMaintainer(smart_api=smart_api)
    websocket_manager.strategy_maintainer = strategy_maintainer

    websocket_manager.executor = executor
    time.sleep(5)
    # Subscribe to an instrument
    instrument = "3045"
    exchange = 1
    executor.submit(asyncio.run, websocket_manager.subscribe_to_instrument(instrument, exchange)) 
    
    # Add strategy for the instrument
    timeframe = "1m"
    strategy_name = "SuperTrend"
    parameters = {
        "atr_length": 14,
        "atr_multiplier": 3,
        "length": 10
    }
    strategy_maintainer.add_strategy(instrument, timeframe, strategy_name, parameters)

    time.sleep(60)

    strategy_maintainer.stop_all_strategies()

    time.sleep(2)

    strategy_maintainer.add_strategy(instrument, timeframe, strategy_name, parameters)

    time.sleep(10)

    strategy_maintainer.stop_strategy(instrument, timeframe)
    # Keep the WebSocket running
    print("WebSocket is set up, instrument subscribed, and strategy added.")
    print("Press Ctrl+C to exit.")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__ == "__main__":
    asyncio.run(main())
