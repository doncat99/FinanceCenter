import warnings
warnings.filterwarnings("ignore")

import logging
import os
import platform
import enum
import asyncio
import time
from datetime import datetime

from tqdm.auto import tqdm

from findy.interface import Region, Provider, RunMode
from findy.utils.cache import valid, get_cache, dump_cache
import findy.vendor.aiomultiprocess as amp

logger = logging.getLogger(__name__)


class interface():
    @staticmethod
    async def get_stock_list_data(region: Region, provider: Provider, sleep, process, desc):
        # 股票列表
        from findy.database.schema.meta.stock_meta import Stock
        await Stock.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_etf_list_data(region: Region, provider: Provider, sleep, process, desc):
        from findy.database.schema.meta.stock_meta import Etf
        await Etf.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_trade_day(region: Region, provider: Provider, sleep, process, desc):
        # 交易日
        from findy.database.schema.quotes.trade_day import StockTradeDay
        await StockTradeDay.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_main_index(region: Region, provider: Provider, sleep, process, desc):
        from findy.database.plugins.exchange.main_index import init_main_index
        await init_main_index(region, provider)

    @staticmethod
    async def get_stock_summary_data(region: Region, provider: Provider, sleep, process, desc):
        # 市场整体估值
        from findy.database.schema.misc.overall import StockSummary
        await StockSummary.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_detail_data(region: Region, provider: Provider, sleep, process, desc):
        # 个股详情
        from findy.database.schema.meta.stock_meta import StockDetail
        await StockDetail.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_finance_factor_data(region: Region, provider: Provider, sleep, process, desc):
        # 主要财务指标
        from findy.database.schema.fundamental.finance import FinanceFactor
        await FinanceFactor.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_balance_sheet_data(region: Region, provider: Provider, sleep, process, desc):
        # 资产负债表
        from findy.database.schema.fundamental.finance import BalanceSheet
        await BalanceSheet.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_income_statement_data(region: Region, provider: Provider, sleep, process, desc):
        # 收益表
        from findy.database.schema.fundamental.finance import IncomeStatement
        await IncomeStatement.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_cashflow_statement_data(region: Region, provider: Provider, sleep, process, desc):
        # 现金流量表
        from findy.database.schema.fundamental.finance import CashFlowStatement
        await CashFlowStatement.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_moneyflow_data(region: Region, provider: Provider, sleep, process, desc):
        # 股票资金流向表
        from findy.database.schema.misc.money_flow import StockMoneyFlow
        await StockMoneyFlow.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_dividend_financing_data(region: Region, provider: Provider, sleep, process, desc):
        # 除权概览表
        from findy.database.schema.fundamental.dividend_financing import DividendFinancing
        await DividendFinancing.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_dividend_detail_data(region: Region, provider: Provider, sleep, process, desc):
        # 除权具细表
        from findy.database.schema.fundamental.dividend_financing import DividendDetail
        await DividendDetail.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_rights_issue_detail_data(region: Region, provider: Provider, sleep, process, desc):
        # 配股表
        from findy.database.schema.fundamental.dividend_financing import RightsIssueDetail
        await RightsIssueDetail.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_spo_detail_data(region: Region, provider: Provider, sleep, process, desc):
        # 现金增资
        from findy.database.schema.fundamental.dividend_financing import SpoDetail
        await SpoDetail.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_margin_trading_summary_data(region: Region, provider: Provider, sleep, process, desc):
        # 融资融券概况
        from findy.database.schema.misc.overall import MarginTradingSummary
        await MarginTradingSummary.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_cross_market_summary_data(region: Region, provider: Provider, sleep, process, desc):
        # 北向/南向成交概况
        from findy.database.schema.misc.overall import CrossMarketSummary
        await CrossMarketSummary.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_holder_trading_data(region: Region, provider: Provider, sleep, process, desc):
        # 股东交易
        from findy.database.schema.fundamental.trading import HolderTrading
        await HolderTrading.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_top_ten_holder_data(region: Region, provider: Provider, sleep, process, desc):
        # 前十股东表
        from findy.database.schema.misc.holder import TopTenHolder
        await TopTenHolder.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_top_ten_tradable_holder_data(region: Region, provider: Provider, sleep, process, desc):
        # 前十可交易股东表
        from findy.database.schema.misc.holder import TopTenTradableHolder
        await TopTenTradableHolder.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_valuation_data(region: Region, provider: Provider, sleep, process, desc):
        # 个股估值数据
        from findy.database.schema.fundamental.valuation import StockValuation
        await StockValuation.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_etf_stock_data(region: Region, provider: Provider, sleep, process, desc):
        # ETF股票
        from findy.database.schema.meta.stock_meta import EtfStock
        await EtfStock.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_etf_valuation_data(region: Region, provider: Provider, sleep, process, desc):
        # ETF估值数据
        from findy.database.schema.fundamental.valuation import EtfValuation
        await EtfValuation.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1d_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 日线
        from findy.database.schema.quotes.stock.stock_1d_kdata import Stock1dKdata
        await Stock1dKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1d_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 日线复权
        from findy.database.schema.quotes.stock.stock_1d_kdata import Stock1dHfqKdata
        await Stock1dHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1w_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 周线
        from findy.database.schema.quotes.stock.stock_1wk_kdata import Stock1wkKdata
        await Stock1wkKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1w_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 周线复权
        from findy.database.schema.quotes.stock.stock_1wk_kdata import Stock1wkHfqKdata
        await Stock1wkHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1mon_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 月线
        from findy.database.schema.quotes.stock.stock_1mon_kdata import Stock1monKdata
        await Stock1monKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1mon_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 月线复权
        from findy.database.schema.quotes.stock.stock_1mon_kdata import Stock1monHfqKdata
        await Stock1monHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1m_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 1分钟线
        from findy.database.schema.quotes.stock.stock_1m_kdata import Stock1mKdata
        await Stock1mKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 1分钟线复权
        from findy.database.schema.quotes.stock.stock_1m_kdata import Stock1mHfqKdata
        await Stock1mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_5m_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 5分钟线
        from findy.database.schema.quotes.stock.stock_5m_kdata import Stock5mKdata
        await Stock5mKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_5m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 5分钟线复权
        from findy.database.schema.quotes.stock.stock_5m_kdata import Stock5mHfqKdata
        await Stock5mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_15m_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 15分钟线
        from findy.database.schema.quotes.stock.stock_15m_kdata import Stock15mKdata
        await Stock15mKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_15m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 15分钟线复权
        from findy.database.schema.quotes.stock.stock_15m_kdata import Stock15mHfqKdata
        await Stock15mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_30m_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 30分钟线
        from findy.database.schema.quotes.stock.stock_30m_kdata import Stock30mKdata
        await Stock30mKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_30m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 30分钟线复权
        from findy.database.schema.quotes.stock.stock_30m_kdata import Stock30mHfqKdata
        await Stock30mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1h_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 1小时线
        from findy.database.schema.quotes.stock.stock_1h_kdata import Stock1hKdata
        await Stock1hKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_stock_1h_hfq_k_data(region: Region, provider: Provider, sleep, process, desc):
        # 1小时线复权
        from findy.database.schema.quotes.stock.stock_1h_kdata import Stock1hHfqKdata
        await Stock1hHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_etf_1d_k_data(region: Region, provider: Provider, sleep, process, desc):
        from findy.database.schema.quotes.etf.etf_1d_kdata import Etf1dKdata
        await Etf1dKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)

    @staticmethod
    async def get_index_1d_k_data(region: Region, provider: Provider, sleep, process, desc):
        from findy.database.schema.quotes.index.index_1d_kdata import Index1dKdata
        await Index1dKdata.record_data(region=region, provider=provider, share_para=(process, desc), sleeping_time=sleep)


class Para(enum.Enum):
    FunName = 0
    Provider = 1
    Sleep = 2
    Processor = 3
    Desc = 4
    Cache = 5
    Mode = 6


data_set_chn = [
    [interface.get_stock_list_data,              Provider.Exchange,  0, 10, "Stock List",               24 * 6, RunMode.Serial],
    [interface.get_stock_trade_day,              Provider.BaoStock,  0, 10, "Trade Day",                24,     RunMode.Serial],
    # [interface.get_etf_list_data,                Provider.BaoStock,  0, 10, "Etf List",                 24 * 6, RunMode.Serial],
    [interface.get_stock_main_index,             Provider.BaoStock,  0, 10, "Main Index",               24,     RunMode.Serial],
    [interface.get_stock_detail_data,            Provider.TuShare, 0, 4, "Stock Detail",             24 * 6, RunMode.Parallel],

    # [interface.get_dividend_financing_data,      Provider.EastMoney, 0, 10, "Divdend Financing",        24 * 6,  RunMode.Parallel],
    # [interface.get_top_ten_holder_data,          Provider.EastMoney, 0, 10, "Top Ten Holder",           24 * 6,  RunMode.Parallel],
    # [interface.get_top_ten_tradable_holder_data, Provider.EastMoney, 0, 10, "Top Ten Tradable Holder",  24 * 6,  RunMode.Parallel],
    # [interface.get_dividend_detail_data,         Provider.EastMoney, 0, 10, "Divdend Detail",           24 * 6,  RunMode.Parallel],
    # [interface.get_spo_detail_data,              Provider.EastMoney, 0, 10, "SPO Detail",               24 * 6,  RunMode.Parallel],
    # [interface.get_rights_issue_detail_data,     Provider.EastMoney, 0, 10, "Rights Issue Detail",      24,      RunMode.Parallel],
    # [interface.get_holder_trading_data,          Provider.EastMoney, 0, 10, "Holder Trading",           24 * 6,  RunMode.Parallel],

    # # below functions call join-quant sdk interface which limit at most 3 concurrent request
    # [interface.get_finance_factor_data,          Provider.EastMoney, 0, 10, "Finance Factor",           24 * 6,  RunMode.Parallel],
    # [interface.get_balance_sheet_data,           Provider.EastMoney, 0, 10, "Balance Sheet",            24 * 6,  RunMode.Parallel],
    # [interface.get_income_statement_data,        Provider.EastMoney, 0, 10, "Income Statement",         24 * 6,  RunMode.Parallel],
    # [interface.get_cashflow_statement_data,      Provider.EastMoney, 0, 10, "CashFlow Statement",       24,      RunMode.Parallel],
    # [interface.get_stock_valuation_data,         Provider.JoinQuant, 0, 10, "Stock Valuation",          24,      RunMode.Parallel],
    # [interface.get_cross_market_summary_data,    Provider.JoinQuant, 0, 10, "Cross Market Summary",     24,      RunMode.Parallel],
    # [interface.get_etf_stock_data,               Provider.JoinQuant, 0, 10, "ETF Stock",                24,      RunMode.Parallel],
    # [interface.get_stock_summary_data,           Provider.Exchange,  0, 10, "Stock Summary",            24,      RunMode.Parallel],
    # [interface.get_margin_trading_summary_data,  Provider.JoinQuant, 0, 10, "Margin Trading Summary",   24,      RunMode.Parallel],
    # [interface.get_etf_valuation_data,           Provider.JoinQuant, 0, 10, "ETF Valuation",            24,      RunMode.Parallel],
    # [interface.get_moneyflow_data,               Provider.Sina,      1, 10, "MoneyFlow Statement",      24,      RunMode.Parallel],

    # [interface.get_etf_1d_k_data,                Provider.Sina,      0, 10, "ETF Daily K-Data",         24,      RunMode.Parallel],
    [interface.get_stock_1d_k_data,              Provider.BaoStock,  0, 10, "Stock Daily   K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1w_k_data,              Provider.BaoStock,  0, 10, "Stock Weekly  K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1mon_k_data,            Provider.BaoStock,  0, 10, "Stock Monthly K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1h_k_data,              Provider.BaoStock,  0, 10, "Stock 1 hours K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_30m_k_data,             Provider.BaoStock,  0, 10, "Stock 30 mins K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_15m_k_data,             Provider.BaoStock,  0, 10, "Stock 15 mins K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_5m_k_data,              Provider.BaoStock,  0, 10, "Stock 5 mins  K-Data",     24,      RunMode.Parallel],
    # [interface.get_stock_1m_k_data,              Provider.BaoStock,  0, 10, "Stock 1 mins  K-Data",     24,      RunMode.Parallel],

    # [interface.get_stock_1d_hfq_k_data,          Provider.BaoStock,  0, 10, "Stock Daily   HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_1w_hfq_k_data,          Provider.BaoStock,  0, 10, "Stock Weekly  HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_1mon_hfq_k_data,        Provider.BaoStock,  0, 10, "Stock Monthly HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_1h_hfq_k_data,          Provider.BaoStock,  0, 10, "Stock 1 hours HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_30m_hfq_k_data,         Provider.BaoStock,  0, 10, "Stock 30 mins HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_15m_hfq_k_data,         Provider.BaoStock,  0, 10, "Stock 15 mins HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_5m_hfq_k_data,          Provider.BaoStock,  0, 10, "Stock 5 mins  HFQ K-Data", 24,      RunMode.Parallel],
    # [interface.get_stock_1m_hfq_k_data,          Provider.BaoStock,  0, 10, "Stock 1 mins HFQ K-Data",  24,      RunMode.Parallel],
]

data_set_us = [
    [interface.get_stock_list_data,              Provider.Exchange,  0, 10, "Stock List",               24,      RunMode.Serial],
    [interface.get_stock_trade_day,              Provider.Yahoo,     0, 10, "Trade Day",                24,      RunMode.Serial],
    [interface.get_stock_main_index,             Provider.Exchange,  0, 10, "Main Index",               24,      RunMode.Serial],
    # [interface.get_stock_detail_data,            Provider.Yahoo,     0, 10, "Stock Detail",             24 * 6,  RunMode.Parallel],

    [interface.get_index_1d_k_data,              Provider.Yahoo,     0, 20, "Index Daily   K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1d_k_data,              Provider.Yahoo,     0, 20, "Stock Daily   K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1w_k_data,              Provider.Yahoo,     0, 20, "Stock Weekly  K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1mon_k_data,            Provider.Yahoo,     0, 20, "Stock Monthly K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1h_k_data,              Provider.Yahoo,     0, 20, "Stock 1 hours K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_30m_k_data,             Provider.Yahoo,     0, 20, "Stock 30 mins K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_15m_k_data,             Provider.Yahoo,     0, 20, "Stock 15 mins K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_5m_k_data,              Provider.Yahoo,     0, 20, "Stock 5 mins  K-Data",     24,      RunMode.Parallel],
    [interface.get_stock_1m_k_data,              Provider.Yahoo,     0, 10, "Stock 1 mins  K-Data",     24,      RunMode.Parallel],
]


async def loop_data_set(args):
    now = time.time()
    region, arg = args
    logger.info(f"Start Func: {arg[Para.FunName.value].__name__}")

    await arg[Para.FunName.value](region, arg[Para.Provider.value], arg[Para.Sleep.value], arg[Para.Processor.value], arg[Para.Desc.value])
    logger.info(f"End Func: {arg[Para.FunName.value].__name__}, cost: {time.time() - now}\n")

    return arg[Para.FunName.value].__name__

    # try:
    #     arg[Para.FunName.value](region, arg[Para.Provider.value], arg[Para.Sleep.value], arg[Para.Processor.value], arg[Para.Desc.value])
    #     logger.info(f"End Func: {arg[Para.FunName.value].__name__}, cost: {time.time() - now}\n")
    #     return arg[Para.FunName.value].__name__
    # except Exception as e:
    #     logger.error(f"End Func: {arg[Para.FunName.value].__name__}, cost: {time.time() - now}, errors: {e}\n")
    # return None


async def fetch_data(region: Region):
    print("")
    print("*" * 80)
    print(f"*    Start Fetching {region.value.upper()} Stock information...      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("*" * 80)

    if region == Region.CHN:
        data_set = data_set_chn
    elif region == Region.US:
        data_set = data_set_us
    else:
        data_set = []

    print("")
    print("parallel fetching processing...")
    print("")

    cache = get_cache('cache')
    calls_list = []
    index = 1
    for item in data_set:
        if not valid(region, item[Para.FunName.value].__name__, item[Para.Cache.value], cache):
            if item[Para.Mode.value] == RunMode.Serial:
                item[Para.Desc.value] = (0, item[Para.Desc.value])
            else:
                item[Para.Desc.value] = (index, item[Para.Desc.value])
                index += 1
            calls_list.append((region, item))

    # calls_list = [(region, item) for item in data_set if not valid(region, item[Para.FunName.value].__name__, item[Para.Cache.value], cache)]

    Multi = True

    if Multi:
        pool_tasks = []
        tasks = len(calls_list)
        cpus = min(tasks, os.cpu_count())
        childconcurrency = round(tasks / cpus)

        current_os = platform.system().lower()
        if current_os != "windows":
            import uvloop
            loop_initializer = uvloop.new_event_loop
        else:
            loop_initializer = None

        async with amp.Pool(cpus, childconcurrency=childconcurrency, loop_initializer=loop_initializer) as pool:
            for call in calls_list:
                if call[1][Para.Mode.value] == RunMode.Serial:
                    result = await pool.apply(loop_data_set, args=[call])
                    # cache.update({f"{region.value}_{result}": datetime.now()})
                    # dump_cache('cache', cache)
                else:
                    pool_tasks.append(pool.apply(loop_data_set, args=[call]))

            pbar = tqdm(total=len(pool_tasks), ncols=90, desc="Parallel Jobs", position=0, leave=False)
            for result in asyncio.as_completed(pool_tasks):
                await result
                # cache.update({f"{region.value}_{result}": datetime.now()})
                # dump_cache('cache', cache)
                pbar.update()

            # for result in tqdm.as_completed(pool_tasks, ncols=90, desc="Parallel Jobs", position=0, leave=True):
            #     await result
            #     cache.update({f"{region.value}_{result}": datetime.now()})
            #     # dump_cache('cache', cache)

    else:
        for call in calls_list:
            await loop_data_set(call)


def fetching(region: Region):
    asyncio.run(fetch_data(region))


if __name__ == '__main__':
    fetching(Region.US)
