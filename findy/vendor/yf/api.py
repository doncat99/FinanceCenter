import logging
import json
import time
import datetime
from enum import Enum

from aiohttp import ClientSession
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class APIError(Exception):
    pass


class History(Enum):
    """Defines the possible historical time ranges to be fetched."""

    DAY = "1d"
    FIVE_DAYS = "5d"
    MONTH = "1mo"
    QUARTER = "3mo"
    HALF_YEAR = "6mo"
    YEAR = "1y"
    TWO_YEARS = "2y"
    FIVE_YEARS = "5y"
    TEN_YEARS = "10y"
    YTD = "ytd"
    MAX = "max"


# class Interval(Enum):
#     """Defines the possible lengths of each candle (YH) period."""

#     MINUTE = "1m"
#     TWO_MINUTE = "2m"
#     FIVE_MINUTE = "5m"
#     FIFTEEN_MINUTE = "15m"
#     THIRTY_MINUTE = "30m"
#     HOUR = "1h"
#     DAY = "1d"
#     FIVE_DAY = "5d"
#     WEEK = "1wk"
#     MONTH = "1mo"
#     THREE_MONTH = "3mo"


class YH:
    """API call to fetch the YH candles."""

    base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
    base_params = {
        "events": "history",
    }

    @classmethod
    async def fetch(
        cls,
        http_session: ClientSession,
        symbol: str,
        interval: str,
        start: datetime.datetime = None,
        end: datetime.datetime = None,
        period: History = History.MONTH,
        prepost: bool = False,
        actions=True,
        proxy=None,
        auto_adjust=True,
        back_adjust=False,
        rounding=False,
        tz=None,
        **kwargs,
    ):
        """An async method that fetches the YH data, parses it and returns as a list of candles.
        Parameters:
        - symbol - the name of the stock on Yahoo Finance e.g. AAPL, AV.L
        - interval - the length of the candles (an Interval Enum)
        - history - how far to go back to fetch candles (a History Enum)
        Return format:
        {
            "candles":
            [
                {datetime: candle_open_date, "open": 232, "high": 236.5, "low": 213.44, "close": 225.45, "volume": 34.2},
                {...}
            ],
            "meta" : the list of meta data returned by the API for checking purposes
        }
        """
        params = cls.base_params.copy()

        """Converts the response into a list of candle dictionaries.
        Returns the parsed candles and the meta data."""
        if start or period is None or period.lower() == "max":
            if start is None:
                start = -2208988800
            elif isinstance(start, datetime.datetime):
                start = int(time.mktime(start.timetuple()))
            else:
                start = int(time.mktime(
                    time.strptime(str(start), '%Y-%m-%d')))
            if end is None:
                end = int(time.time())
            elif isinstance(end, datetime.datetime):
                end = int(time.mktime(end.timetuple()))
            else:
                end = int(time.mktime(time.strptime(str(end), '%Y-%m-%d')))

            params = {"period1": start, "period2": end}
        else:
            period = period.lower()
            params = {"range": period}

        params["interval"] = interval.lower()
        params["includePrePost"] = "true" if prepost else "false"
        params["events"] = "div,splits"

        # 1) fix weired bug with Yahoo! - returning 60m for 30m bars
        if params["interval"] == "30m":
            params["interval"] = "15m"

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        text, msg = await cls.raw_fetch(http_session, symbol, params, proxy)

        if text is None:
            logger.debug(f'{msg}')
            return None, msg

        return cls.parse_ohlc(symbol, text, params["interval"], actions, auto_adjust, back_adjust, rounding, tz)

    @classmethod
    async def raw_fetch(cls, http_session, ticker, params, proxy) -> dict:
        """Fetches from the API and returns the response in the original format.
        Does basic error checking."""
        url = cls.base_url + ticker
        async with http_session.get(url, params=params, proxy=proxy) as response:
            text = await response.text()
            if "Will be right back" in text:
                msg = "YAHOO! FINANCE IS CURRENTLY DOWN! Our engineers are working quickly to resolve the issue. Thank you for your patience."
                return None, f'{ticker}, {msg}'
            if "HTTP 404 Not Found" in text:
                msg = "HTTP 404 Not Found"
                return None, f'{ticker}, {msg}'
            return text, ""

    @classmethod
    def parse_ohlc(cls, ticker, text, interval, actions, auto_adjust, back_adjust, rounding, tz) -> tuple:
        """Converts the response into a list of candle dictionaries.
        Returns the parsed candles and the meta data."""

        try:
            data = json.loads(text)
        except Exception as e:
            err_msg = "decode json failed with code: {}, error: {}".format(ticker, e)
            logger.debug(err_msg)
            return None, err_msg

        err_msg = "No data found for this date range, symbol may be delisted"
        if "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            logger.debug('- %s: %s' % (ticker, err_msg))
            return None, err_msg

        elif "chart" not in data or data["chart"]["result"] is None or \
                not data["chart"]["result"]:
            logger.debug('- %s: %s' % (ticker, err_msg))
            return None, err_msg

        # parse quotes
        err_msg = "parse data failed"
        try:
            quotes = YH.parse_quotes(data["chart"]["result"][0], tz)
        except Exception:
            logger.debug('- %s: %s' % (ticker, err_msg))
            return None, err_msg

        # 2) fix weired bug with Yahoo! - returning 60m for 30m bars
        if interval.lower() == "30m":
            quotes2 = quotes.resample('30T')
            quotes = pd.DataFrame(index=quotes2.last().index, data={
                'Open': quotes2['Open'].first(),
                'High': quotes2['High'].max(),
                'Low': quotes2['Low'].min(),
                'Close': quotes2['Close'].last(),
                'Adj Close': quotes2['Adj Close'].last(),
                'Volume': quotes2['Volume'].sum()
            })
            try:
                quotes['Dividends'] = quotes2['Dividends'].max()
            except Exception:
                pass
            try:
                quotes['Stock Splits'] = quotes2['Dividends'].max()
            except Exception:
                pass

        if auto_adjust:
            quotes = YH.auto_adjust(quotes)
        elif back_adjust:
            quotes = YH.back_adjust(quotes)

        if rounding:
            quotes = np.round(quotes, data["chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(np.int64)

        quotes.dropna(inplace=True)

        # actions
        dividends, splits = YH.parse_actions(data["chart"]["result"][0], tz)

        # combine
        df = pd.concat([quotes, dividends, splits], axis=1, sort=True)
        df["Dividends"].fillna(0, inplace=True)
        df["Stock Splits"].fillna(0, inplace=True)

        # index eod/intraday
        df.index = df.index.tz_localize("UTC").tz_convert(
            data["chart"]["result"][0]["meta"]["exchangeTimezoneName"])

        if interval[-1] == "m":
            df.index.name = "Datetime"
        else:
            df.index = pd.to_datetime(df.index.date)
            if tz is not None:
                df.index = df.index.tz_localize(tz)
            df.index.name = "Date"

        if not actions:
            cols = ["Dividends", "Stock Splits"]
            for col in cols:
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)

        return df, ""

    @staticmethod
    def parse_quotes(data, tz=None):
        timestamps = data["timestamp"]
        ohlc = data["indicators"]["quote"][0]
        volumes = ohlc["volume"]
        opens = ohlc["open"]
        closes = ohlc["close"]
        lows = ohlc["low"]
        highs = ohlc["high"]

        adjclose = closes
        if "adjclose" in data["indicators"]:
            adjclose = data["indicators"]["adjclose"][0]["adjclose"]

        quotes = pd.DataFrame({"Open": opens,
                               "High": highs,
                               "Low": lows,
                               "Close": closes,
                               "Adj Close": adjclose,
                               "Volume": volumes})

        quotes.index = pd.to_datetime(timestamps, unit="s")
        quotes.sort_index(inplace=True)

        if tz is not None:
            quotes.index = quotes.index.tz_localize(tz)

        return quotes

    @staticmethod
    def auto_adjust(data):
        df = data.copy()
        ratio = df["Close"] / df["Adj Close"]
        df["Adj Open"] = df["Open"] / ratio
        df["Adj High"] = df["High"] / ratio
        df["Adj Low"] = df["Low"] / ratio

        df.drop(["Open", "High", "Low", "Close"], axis=1, inplace=True)

        df.rename(columns={
            "Adj Open": "Open", "Adj High": "High",
            "Adj Low": "Low", "Adj Close": "Close"
        }, inplace=True)

        df = df[["Open", "High", "Low", "Close", "Volume"]]
        return df[["Open", "High", "Low", "Close", "Volume"]]

    @staticmethod
    def back_adjust(data):
        """ back-adjusted data to mimic true historical prices """

        df = data.copy()
        ratio = df["Adj Close"] / df["Close"]
        df["Adj Open"] = df["Open"] * ratio
        df["Adj High"] = df["High"] * ratio
        df["Adj Low"] = df["Low"] * ratio

        df.drop(
            ["Open", "High", "Low", "Adj Close"],
            axis=1, inplace=True)

        df.rename(columns={
            "Adj Open": "Open", "Adj High": "High",
            "Adj Low": "Low"
        }, inplace=True)

        return df[["Open", "High", "Low", "Close", "Volume"]]

    @staticmethod
    def parse_actions(data, tz=None):
        dividends = pd.DataFrame(columns=["Dividends"])
        splits = pd.DataFrame(columns=["Stock Splits"])

        if "events" in data:
            if "dividends" in data["events"]:
                dividends = pd.DataFrame(
                    data=list(data["events"]["dividends"].values()))
                dividends.set_index("date", inplace=True)
                dividends.index = pd.to_datetime(dividends.index, unit="s")
                dividends.sort_index(inplace=True)
                if tz is not None:
                    dividends.index = dividends.index.tz_localize(tz)

                dividends.columns = ["Dividends"]

            if "splits" in data["events"]:
                splits = pd.DataFrame(
                    data=list(data["events"]["splits"].values()))
                splits.set_index("date", inplace=True)
                splits.index = pd.to_datetime(splits.index, unit="s")
                splits.sort_index(inplace=True)
                if tz is not None:
                    splits.index = splits.index.tz_localize(tz)
                splits["Stock Splits"] = splits["numerator"] / \
                    splits["denominator"]
                splits = splits["Stock Splits"]

        return dividends, splits
