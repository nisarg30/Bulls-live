import pandas
import pandas_ta as ta

def SuperTrend(df, atr_length, atr_multiplier, length):
    """
    Calculates the Supertrend and determines buy/sell signals for the last candle.

    :param df: The DataFrame containing instrument data.
    :param atr_length: The length for ATR calculation in the Supertrend.
    :param atr_multiplier: The multiplier for ATR in the Supertrend.
    :param length: The lookback period for determining higher high and lower low.
    :return: "buy", "sell", or "neutral" based on the last candle's conditions.
    """
    # Ensure necessary columns are present
    required_columns = {"high", "low", "close"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"DataFrame must contain the following columns: {required_columns}")

    # Calculate Supertrend for the entire DataFrame
    supertrend = ta.supertrend(high=df["high"], low=df["low"], close=df["close"],
                               length=atr_length, multiplier=atr_multiplier)

    # Correct column name format, considering the '.0' suffix
    supertrend_column = f"SUPERT_{atr_length}_{atr_multiplier}.0"  # Fixed column name format
    supertrend_direction_column = f"SUPERTd_{atr_length}_{atr_multiplier}.0"  # Fixed column name format

    print(supertrend, supertrend_column, supertrend_direction_column)
    # Ensure columns exist
    if supertrend_column not in supertrend.columns or supertrend_direction_column not in supertrend.columns:
        raise ValueError(f"Expected columns {supertrend_column} and {supertrend_direction_column} are missing from the Supertrend output.")

    data = pandas.DataFrame()
    # Add Supertrend columns to the DataFrame using .loc to avoid SettingWithCopyWarning
    data.loc[:, "supertrend"] = supertrend[supertrend_column]
    data.loc[:, "supertrend_direction"] = supertrend[supertrend_direction_column]

    # Calculate the rolling highest close and lowest close for the last `length` candles
    highest_close = df["close"].iloc[-length:].max()
    lowest_close = df["close"].iloc[-length:].min()

    # Determine higher high and lower low conditions for the last candle
    higher_high = df["close"].iloc[-1] > highest_close
    lower_low = df["close"].iloc[-1] < lowest_close

    # Determine buy, sell, or neutral
    if data["supertrend_direction"].iloc[-1] > 0 and higher_high:
        return "buy"
    elif data["supertrend_direction"].iloc[-1] < 0 and lower_low:
        return "sell"
    else:
        return "neutral"
