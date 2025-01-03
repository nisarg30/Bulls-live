import pandas as pd
import threading
import importlib
from datetime import datetime, timedelta

class StrategyMaintainer:
    def __init__(self, smart_api):
        """
        Initializes the StrategyMaintainer.
        :param smart_api: An instance of the SmartAPI client for fetching data.
        """
        self.instrument_timeframe_map = {}
        self.dataframes = {}
        self.lock = threading.Lock()
        self.smart_api = smart_api

    def fetch_historical_data(self, symbol_token, interval, from_date, to_date, exchange = "NSE"):
        """
        Fetches historical data for the given instrument and interval.

        :param symbol_token: The instrument token.
        :param interval: Time interval for the historical data (e.g., "ONE_MINUTE").
        :param from_date: Start date in the format "YYYY-MM-DD HH:MM".
        :param to_date: End date in the format "YYYY-MM-DD HH:MM".
        :return: A DataFrame containing historical OHLC data.
        """
        try:
            historical_params = {
                "exchange": exchange,
                "symboltoken": symbol_token,
                "interval": interval,
                "fromdate": from_date,
                "todate": to_date
            }
            candle_data = self.smart_api.getCandleData(historical_params)
            return self.process_historical_data(candle_data['data'])
        except Exception as e:
            print(f"Error fetching historical data for {symbol_token}: {e}")
            return pd.DataFrame()

    def process_historical_data(self, candle_data):
        """
        Processes raw historical candle data into a DataFrame.

        :param candle_data: Raw candle data.
        :return: Processed DataFrame.
        """
        processed_data = []
        for candle in candle_data:
            processed_data.append({
                "timestamp": datetime.fromisoformat(candle[0][:-6]),
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
            })
        return pd.DataFrame(processed_data)

    def add_strategy(self, instrument: str, timeframe: str, strategy_name: str, parameters: dict):
        """
        Adds a strategy and initializes the DataFrame with historical data.

        :param instrument: The instrument name (e.g., "AAPL").
        :param timeframe: The timeframe (e.g., "5m").
        :param strategy_name: The name of the strategy function.
        :param parameters: Strategy parameters.
        """
        with self.lock:
            if instrument not in self.instrument_timeframe_map:
                self.instrument_timeframe_map[instrument] = {}
                self.dataframes[instrument] = {}

            # Set up historical data
            # token_map = {"NIFTY": "99926000"}  # Replace with your actual token mapping
            symbol_token = instrument
            if not symbol_token:
                print(f"Instrument {instrument} token not found!")
                return
            
            timeframe_map = {
                "1m": "ONE_MINUTE",
                "3m": "THREE_MINUTE",
                "5m": "FIVE_MINUTE",
                "15m": "FIFTEEN_MINUTE",
                "30m": "THIRTY_MINUTE",
                "1h": "ONE_HOUR",
            }

            ttf = timeframe_map.get(timeframe)

            current_date = datetime.now() + timedelta(days = 1)
            to_date = current_date.replace(hour=3, minute=30, second=0, microsecond=0)
            from_date = (current_date - timedelta(days=2)).replace(hour=9, minute=15, second=0, microsecond=0)
            from_date_str = from_date.strftime("%Y-%m-%d %H:%M")
            to_date_str = to_date.strftime("%Y-%m-%d %H:%M")

            historical_data = self.fetch_historical_data(
                symbol_token, ttf, from_date_str, to_date_str
            )

            print(historical_data)
            if timeframe not in self.dataframes[instrument]:
                self.dataframes[instrument][timeframe] = historical_data

            # Save the strategy details
            self.instrument_timeframe_map[instrument][timeframe] = (strategy_name, parameters)
            print(f"Strategy '{strategy_name}' added for {instrument} with timeframe {timeframe}.")

    def stop_strategy(self, instrument: str, timeframe: str):
        """
        Stops a specific strategy for the given instrument and timeframe.

        :param instrument: The instrument name.
        :param timeframe: The timeframe (e.g., "5m").
        """
        with self.lock:
            if instrument in self.instrument_timeframe_map and \
                    timeframe in self.instrument_timeframe_map[instrument]:
                del self.instrument_timeframe_map[instrument][timeframe]
                print(f"Stopped strategy for {instrument} with timeframe {timeframe}.")
                # Optionally clear the DataFrame for this timeframe
                if instrument in self.dataframes and timeframe in self.dataframes[instrument]:
                    del self.dataframes[instrument][timeframe]
            else:
                print(f"No strategy found for {instrument} with timeframe {timeframe}.")

    def stop_all_strategies(self):
        """
        Stops all strategies for all instruments and timeframes.
        """
        with self.lock:
            self.instrument_timeframe_map.clear()
            self.dataframes.clear()
            print("Stopped all strategies for all instruments and timeframes.")
            
    def update_dataframes(self, instrument: str, tick_data: dict):
        """
        Updates the dataframes for all timeframes associated with an instrument using new tick data.

        :param instrument: The instrument name (e.g., "AAPL").
        :param tick_data: A dictionary containing the tick data (e.g., {"timestamp": ..., "price": ...}).
        """
        with self.lock:
            if instrument not in self.dataframes:
                print(f"No timeframes found for instrument: {instrument}")
                return

            tick_timestamp = pd.to_datetime(tick_data["timestamp"], unit='s')  # Ensure proper conversion
            tick_price = tick_data["price"]

            timeframe_map = {
                "1m": "min",
                "3m": "3min",
                "5m": "5min",
                "15m": "15min",
                "30m": "30min",
                "1h": "H",
            }

            # Update all timeframes for the instrument
            for timeframe, df in self.dataframes[instrument].items():
                timeframe_duration = timeframe_map.get(timeframe)
                if timeframe_duration is None:
                    print(f"Unsupported timeframe: {timeframe} for instrument: {instrument}")
                    continue

                # Calculate the current timeframe boundary
                timeframe_start = tick_timestamp.floor(timeframe_duration)
                new_row_added = False

                if not df.empty and pd.to_datetime(df.iloc[-1]["timestamp"]) == timeframe_start:
                    # Update the existing row for the current timeframe
                    df.at[df.index[-1], "high"] = max(df.at[df.index[-1], "high"], tick_price)
                    df.at[df.index[-1], "low"] = min(df.at[df.index[-1], "low"], tick_price)
                    df.at[df.index[-1], "close"] = tick_price
                else:
                    # Add a new row for the new timeframe
                    new_row = {
                        "timestamp": timeframe_start,
                        "open": tick_price,
                        "high": tick_price,
                        "low": tick_price,
                        "close": tick_price
                    }
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    self.dataframes[instrument][timeframe] = df
                    new_row_added = True

                # Execute the strategy only on old rows
                if new_row_added and instrument in self.instrument_timeframe_map and \
                        timeframe in self.instrument_timeframe_map[instrument]:
                    strategy_name, parameters = self.instrument_timeframe_map[instrument][timeframe]

                    # Pass the DataFrame excluding the last row to the strategy execution
                    df_without_last_row = df.iloc[:-1]

                    try:
                        # Dynamically import the strategy function
                        strategies_module = importlib.import_module("strategy")
                        strategy_function = getattr(strategies_module, strategy_name)

                        # Execute the strategy function
                        print(f"Executing strategy on {instrument} with timeframe {timeframe} using parameters: {parameters}")
                        result = strategy_function(df_without_last_row, **parameters)
                        print(result)
                        orderparams = {
                            "variety": "NORMAL",
                            "tradingsymbol": "SBIN-EQ",
                            "symboltoken": "3045",
                            "transactiontype": "BUY",
                            "exchange": "NSE",
                            "ordertype": "MARKET",
                            "producttype": "INTRADAY",
                            "duration": "DAY",
                            "squareoff": "0",
                            "stoploss": "0",
                            "quantity": "10000"
                        }
                        orderId=self.smart_api.placeOrder(orderparams)
                        print("The order id is: {}".format(orderId))
                    except ModuleNotFoundError:
                        print(f"Error: Module 'strategy' not found.")
                    except AttributeError:
                        print(f"Error: Strategy function '{strategy_name}' not found in 'sup' module.")
                    except Exception as e:
                        print(f"Error during strategy execution: {e}")



