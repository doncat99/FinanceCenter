import logging
import json
import time
import datetime
from enum import Enum

from aiohttp import ClientSession
import pandas as pd
import numpy as np

from . import utils

logger = logging.getLogger(__name__)

_BASE_URL_ = 'https://query2.finance.yahoo.com'


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
        tz: str,
        ticker: str,
        interval: str,
        start: datetime.datetime = None,
        end: datetime.datetime = None,
        period: History = History.MONTH,
        prepost: bool = False,
        actions=True,
        proxy=None,
        auto_adjust=True,
        back_adjust=False,
        keepna=False,
        rounding=False,
        **kwargs,
    ):
        """
        :Parameters:
            period : str
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                Either Use period parameter or use start and end
            interval : str
                Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                Intraday data cannot extend last 60 days
            start: str
                Download start date string (YYYY-MM-DD) or _datetime.
                Default is 1900-01-01
            end: str
                Download end date string (YYYY-MM-DD) or _datetime.
                Default is now
            prepost : bool
                Include Pre and Post market data in results?
                Default is False
            auto_adjust: bool
                Adjust all OHLC automatically? Default is True
            back_adjust: bool
                Back-adjusted data to mimic true historical prices
            keepna: bool
                Keep NaN rows returned by Yahoo?
                Default is False
            proxy: str
                Optional. Proxy server URL scheme. Default is None
            rounding: bool
                Round values to 2 decimal places?
                Optional. Default is False = precision suggested by Yahoo!
            timeout: None or float
                If not None stops waiting for a response after given number of
                seconds. (Can also be a fraction of a second e.g. 0.01)
                Default is None.
            **kwargs: dict
                debug: bool
                    Optional. If passed as False, will suppress
                    error message printing to console.
        """

        # Work with errors
        debug_mode = True
        if "debug" in kwargs and isinstance(kwargs["debug"], bool):
            debug_mode = kwargs["debug"]

        err_msg = "No data found for this date range, symbol may be delisted"

        if start or period is None or period.lower() == "max":
            if end is None:
                end = int(time.time())
            else:
                end = utils._parse_user_dt(end, tz)
            if start is None:
                if interval == "1m":
                    start = end - 604800  # Subtract 7 days
                else:
                    start = -631159200
            else:
                start = utils._parse_user_dt(start, tz)
            params = {"period1": start, "period2": end}
        else:
            period = period.lower()
            params = {"range": period}

        params["interval"] = interval.lower()
        # params["includePrePost"] = prepost
        params["events"] = "div,splits"

        # 1) fix weired bug with Yahoo! - returning 60m for 30m bars
        if params["interval"] == "30m":
            params["interval"] = "15m"

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(_BASE_URL_, ticker)

        data = None

        async with http_session.get(url, params=params) as response:
            data = await response.json()
  
        if data is None or not type(data) is dict or 'status_code' in data.keys():
            return utils.empty_df(), err_msg

        if "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            # shared._DFS[self.ticker] = utils.empty_df()
            # shared._ERRORS[self.ticker] = err_msg
            return utils.empty_df(), err_msg

        elif "chart" not in data or data["chart"]["result"] is None or \
                not data["chart"]["result"]:
            # shared._DFS[self.ticker] = utils.empty_df()
            # shared._ERRORS[self.ticker] = err_msg
            return utils.empty_df(), err_msg

        # parse quotes
        try:
            quotes = utils.parse_quotes(data["chart"]["result"][0])
            # Yahoo bug fix - it often appends latest price even if after end date
            if end and not quotes.empty:
                endDt = pd.to_datetime(datetime.datetime.utcfromtimestamp(end))
                if quotes.index[quotes.shape[0]-1] >= endDt:
                    quotes = quotes.iloc[0:quotes.shape[0]-1]
        except Exception:
            # shared._DFS[self.ticker] = utils.empty_df()
            # shared._ERRORS[self.ticker] = err_msg
            return utils.empty_df(), err_msg

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

        try:
            if auto_adjust:
                quotes = utils.auto_adjust(quotes)
            elif back_adjust:
                quotes = utils.back_adjust(quotes)
        except Exception as e:
            if auto_adjust:
                err_msg = "auto_adjust failed with %s" % e
            else:
                err_msg = "back_adjust failed with %s" % e
            # shared._DFS[self.ticker] = utils.empty_df()
            # shared._ERRORS[self.ticker] = err_msg

        if rounding:
            quotes = np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(np.int64)

        if not keepna:
            quotes.dropna(inplace=True)

        # actions
        dividends, splits = utils.parse_actions(data["chart"]["result"][0])

        tz_exchange = data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]

        # combine
        df = pd.concat([quotes, dividends, splits], axis=1, sort=True)
        df["Dividends"].fillna(0, inplace=True)
        df["Stock Splits"].fillna(0, inplace=True)

        # index eod/intraday
        df.index = df.index.tz_localize("UTC").tz_convert(tz_exchange)

        df = utils.fix_Yahoo_dst_issue(df, params["interval"])
            
        if params["interval"][-1] == "m":
            df.index.name = "Datetime"
        elif params["interval"] == "1h":
            pass
        else:
            df.index = pd.to_datetime(df.index.date).tz_localize(tz_exchange)
            df.index.name = "Date"

        # duplicates and missing rows cleanup
        df.dropna(how='all', inplace=True)
        df = df[~df.index.duplicated(keep='first')]

        if not actions:
            df.drop(columns=["Dividends", "Stock Splits"], inplace=True)

        return df, ''
