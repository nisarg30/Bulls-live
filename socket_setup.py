import os
from datetime import datetime
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import asyncio
from dotenv import load_dotenv
from stratagy_maintain import StrategyMaintainer  # Import the StrategyMaintainer class

load_dotenv()

class WebSocketManager:
    def __init__(self, strategy_maintainer: StrategyMaintainer):
        print("WebSocketManager: __init__ method invoked")
        self.sws = None
        self.executor = None
        self.strategies = {}  
        self.subscribed_tokens = {} 
        self.smart_api = None
        self.strategy_maintainer = strategy_maintainer

    def log_with_timestamp(self, message):
        print(f"{datetime.now()}: {message}")

    async def setup_websocket(self, session_data):
        """
        Sets up the Smart API WebSocket connection.
        """
        print("WebSocketManager: setup_websocket method invoked")

        websocket_config = {
            'apikey': os.getenv('apikey'),
            'clientcode': os.getenv('client'),
        }

        self.sws = SmartWebSocketV2(
            session_data['jwtToken'],
            websocket_config['apikey'],
            websocket_config['clientcode'],
            session_data['feedToken']
        )

        def on_data(wsapp, message):
            """
            Handles incoming WebSocket data.
            """
            # print("WebSocketManager: on_data method invoked")
            # print(message)
            self.executor.submit(asyncio.run, self.process_tick(message))

        def on_open(wsapp):
            """
            Handles WebSocket opening.
            """
            print("WebSocketManager: on_open method invoked")
            self.log_with_timestamp("WebSocket connection opened")

        def on_error(wsapp, error):
            """
            Handles WebSocket errors.
            """
            print("WebSocketManager: on_error method invoked")
            self.log_with_timestamp(f"WebSocket error: {error}")

        def on_close(wsapp):
            """
            Handles WebSocket closing.
            """
            print("WebSocketManager: on_close method invoked")
            self.log_with_timestamp("WebSocket connection closed")

        # Set the WebSocket event handlers
        self.sws.on_open = on_open
        self.sws.on_data = on_data
        self.sws.on_error = on_error
        self.sws.on_close = on_close

        try:
            self.sws.connect()
        except Exception as error:
            self.log_with_timestamp(f"WebSocket connection error: {error}")

    async def process_tick(self, message):
        """
        Processes incoming tick data and routes it to subscribed strategies.
        """
        # print("WebSocketManager: process_tick method invoked")
        try:
            # Parse incoming message to get tick data
            tick_data = self.parse_tick_data(message)
            token = tick_data['token']
            self.strategy_maintainer.update_dataframes(token, tick_data)

        except Exception as e:
            self.log_with_timestamp(f"Error processing tick: {e}")

    def parse_tick_data(self, message):
        """
        Parses raw WebSocket data into structured tick data.
        """
        # print("WebSocketManager: parse_tick_data method invoked")
        return {
            "token": message.get("token"),
            "price": message.get("last_traded_price") / 100,
            "timestamp": ((message.get("exchange_timestamp") / 1000) + 19800),
        }

    def subscribe_to_instrument(self, instrument, exchange = 1):
        """
        Subscribes to an instrument via WebSocket.
        """
        print("WebSocketManager: subscribe_to_instrument method invoked")
        self.log_with_timestamp(f"Subscribing to instrument {instrument} with exchange {exchange}")
        token_array = [{"exchangeType": exchange, "tokens": [instrument]}]
        self.sws.subscribe("abcde12345", 2, token_array)
        self.subscribed_tokens[instrument] = exchange

    def unsubscribe_from_instrument(self, instrument, exchange):
        """
        Unsubscribes from an instrument via WebSocket.
        """
        print("WebSocketManager: unsubscribe_from_instrument method invoked")
        self.log_with_timestamp(f"Unsubscribing from {instrument} as no trades are active.")
        token_array = [{"exchangeType": exchange, "tokens": [instrument]}]
        self.sws.unsubscribe("abcde12345", 2, token_array)
        self.subscribed_tokens.pop(instrument, None)
