import logging
import time
import enum
from datetime import datetime

from tqdm.auto import tqdm

from zvt.api.data_type import Region, Provider, RunMode
from zvt.utils.cache_utils import valid, get_cache, dump_cache

logger = logging.getLogger(__name__)


class interface():
    @staticmethod
    def get_stock_list_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 股票列表
        from zvt.domain.meta.stock_meta import Stock
        Stock.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_etf_list_data(region: Region, provider: Provider, sleep, process, desc, mode):
        from zvt.domain.meta.stock_meta import Etf
        Etf.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_trade_day(region: Region, provider: Provider, sleep, process, desc, mode):
        # 交易日
        from zvt.domain.quotes.trade_day import StockTradeDay
        StockTradeDay.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_main_index(region: Region, provider: Provider, sleep, process, desc, mode):
        from zvt.recorders.hardcode.main_index import init_main_index
        init_main_index(region, provider)

    @staticmethod
    def get_stock_summary_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 市场整体估值
        from zvt.domain.misc.overall import StockSummary
        StockSummary.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_detail_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 个股详情
        from zvt.domain.meta.stock_meta import StockDetail
        StockDetail.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_finance_factor_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 主要财务指标
        from zvt.domain.fundamental.finance import FinanceFactor
        FinanceFactor.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_balance_sheet_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 资产负债表
        from zvt.domain.fundamental.finance import BalanceSheet
        BalanceSheet.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_income_statement_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 收益表
        from zvt.domain.fundamental.finance import IncomeStatement
        IncomeStatement.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_cashflow_statement_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 现金流量表
        from zvt.domain.fundamental.finance import CashFlowStatement
        CashFlowStatement.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_moneyflow_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 股票资金流向表
        from zvt.domain.misc.money_flow import StockMoneyFlow
        StockMoneyFlow.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_dividend_financing_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 除权概览表
        from zvt.domain.fundamental.dividend_financing import DividendFinancing
        DividendFinancing.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_dividend_detail_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 除权具细表
        from zvt.domain.fundamental.dividend_financing import DividendDetail
        DividendDetail.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_rights_issue_detail_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 配股表
        from zvt.domain.fundamental.dividend_financing import RightsIssueDetail
        RightsIssueDetail.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_spo_detail_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 现金增资
        from zvt.domain.fundamental.dividend_financing import SpoDetail
        SpoDetail.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_margin_trading_summary_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 融资融券概况
        from zvt.domain.misc.overall import MarginTradingSummary
        MarginTradingSummary.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_cross_market_summary_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 北向/南向成交概况
        from zvt.domain.misc.overall import CrossMarketSummary
        CrossMarketSummary.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_holder_trading_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 股东交易
        from zvt.domain.fundamental.trading import HolderTrading
        HolderTrading.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_top_ten_holder_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 前十股东表
        from zvt.domain.misc.holder import TopTenHolder
        TopTenHolder.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_top_ten_tradable_holder_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 前十可交易股东表
        from zvt.domain.misc.holder import TopTenTradableHolder
        TopTenTradableHolder.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_valuation_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 个股估值数据
        from zvt.domain.fundamental.valuation import StockValuation
        StockValuation.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_etf_stock_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # ETF股票
        from zvt.domain.meta.stock_meta import EtfStock
        EtfStock.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_etf_valuation_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # ETF估值数据
        from zvt.domain.fundamental.valuation import EtfValuation
        EtfValuation.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1d_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 日线
        from zvt.domain.quotes.stock.stock_1d_kdata import Stock1dKdata
        Stock1dKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1d_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 日线复权
        from zvt.domain.quotes.stock.stock_1d_kdata import Stock1dHfqKdata
        Stock1dHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1w_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 周线
        from zvt.domain.quotes.stock.stock_1wk_kdata import Stock1wkKdata
        Stock1wkKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1w_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 周线复权
        from zvt.domain.quotes.stock.stock_1wk_kdata import Stock1wkHfqKdata
        Stock1wkHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1mon_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 月线
        from zvt.domain.quotes.stock.stock_1mon_kdata import Stock1monKdata
        Stock1monKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1mon_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 月线复权
        from zvt.domain.quotes.stock.stock_1mon_kdata import Stock1monHfqKdata
        Stock1monHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1m_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 1分钟线
        from zvt.domain.quotes.stock.stock_1m_kdata import Stock1mKdata
        Stock1mKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 1分钟线复权
        from zvt.domain.quotes.stock.stock_1m_kdata import Stock1mHfqKdata
        Stock1mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_5m_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 5分钟线
        from zvt.domain.quotes.stock.stock_5m_kdata import Stock5mKdata
        Stock5mKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_5m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 5分钟线复权
        from zvt.domain.quotes.stock.stock_5m_kdata import Stock5mHfqKdata
        Stock5mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_15m_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 15分钟线
        from zvt.domain.quotes.stock.stock_15m_kdata import Stock15mKdata
        Stock15mKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_15m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 15分钟线复权
        from zvt.domain.quotes.stock.stock_15m_kdata import Stock15mHfqKdata
        Stock15mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_30m_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 30分钟线
        from zvt.domain.quotes.stock.stock_30m_kdata import Stock30mKdata
        Stock30mKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_30m_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 30分钟线复权
        from zvt.domain.quotes.stock.stock_30m_kdata import Stock30mHfqKdata
        Stock30mHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1h_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 1小时线
        from zvt.domain.quotes.stock.stock_1h_kdata import Stock1hKdata
        Stock1hKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_stock_1h_hfq_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        # 1小时线复权
        from zvt.domain.quotes.stock.stock_1h_kdata import Stock1hHfqKdata
        Stock1hHfqKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)

    @staticmethod
    def get_etf_1d_k_data(region: Region, provider: Provider, sleep, process, desc, mode):
        from zvt.domain.quotes.etf.etf_1d_kdata import Etf1dKdata
        Etf1dKdata.record_data(region=region, provider=provider, share_para=(process, desc, mode), sleeping_time=sleep)


class Para(enum.Enum):
    FunName = 0
    Provider = 1
    Sleep = 2
    Process = 3
    Desc = 4
    Cache = 5
    Mode = 6


data_set_chn = [
    [interface.get_stock_list_data,              Provider.BaoStock,  0, 1, "Stock List",               24*6, RunMode.Sync],
    [interface.get_stock_trade_day,              Provider.BaoStock,  0, 1, "Trade Day",                24,   RunMode.Sync],
    [interface.get_etf_list_data,                Provider.BaoStock,  0, 1, "Etf List",                 24*6, RunMode.Sync],
    [interface.get_stock_main_index,             Provider.BaoStock,  0, 1, "Main Index",               24,   RunMode.Sync],

    [interface.get_dividend_financing_data,      Provider.EastMoney, 0, 0, "Divdend Financing",        24*6, RunMode.Async],
    [interface.get_top_ten_holder_data,          Provider.EastMoney, 0, 0, "Top Ten Holder",           24*6, RunMode.Async],
    [interface.get_top_ten_tradable_holder_data, Provider.EastMoney, 0, 0, "Top Ten Tradable Holder",  24*6, RunMode.Async],
    [interface.get_dividend_detail_data,         Provider.EastMoney, 0, 0, "Divdend Detail",           24*6, RunMode.Async],
    [interface.get_spo_detail_data,              Provider.EastMoney, 0, 0, "SPO Detail",               24*6, RunMode.Async],
    [interface.get_rights_issue_detail_data,     Provider.EastMoney, 0, 0, "Rights Issue Detail",      24,   RunMode.Async],
    [interface.get_holder_trading_data,          Provider.EastMoney, 0, 0, "Holder Trading",           24,   RunMode.Async],
    [interface.get_stock_detail_data,            Provider.EastMoney, 0, 0, "Stock Detail",             24,   RunMode.Async],

    # below functions call join-quant sdk interface which limit at most 1 concurrent request
    [interface.get_finance_factor_data,          Provider.EastMoney, 0, 1, "Finance Factor",           24*6, RunMode.Async],
    [interface.get_balance_sheet_data,           Provider.EastMoney, 0, 1, "Balance Sheet",            24*6, RunMode.Async],
    [interface.get_income_statement_data,        Provider.EastMoney, 0, 1, "Income Statement",         24*6, RunMode.Async],
    [interface.get_cashflow_statement_data,      Provider.EastMoney, 0, 1, "CashFlow Statement",       24,   RunMode.Async],
    [interface.get_stock_valuation_data,         Provider.JoinQuant, 0, 1, "Stock Valuation",          24,   RunMode.Sync],
    [interface.get_cross_market_summary_data,    Provider.JoinQuant, 0, 0, "Cross Market Summary",     24,   RunMode.Sync],
    [interface.get_etf_stock_data,               Provider.JoinQuant, 0, 1, "ETF Stock",                24,   RunMode.Sync],
    [interface.get_stock_summary_data,           Provider.JoinQuant, 0, 1, "Stock Summary",            24,   RunMode.Sync],
    [interface.get_margin_trading_summary_data,  Provider.JoinQuant, 0, 1, "Margin Trading Summary",   24,   RunMode.Sync],
    [interface.get_etf_valuation_data,           Provider.JoinQuant, 0, 1, "ETF Valuation",            24,   RunMode.Sync],
    [interface.get_moneyflow_data,               Provider.Sina,      1, 1, "MoneyFlow Statement",      24,   RunMode.Sync],

    [interface.get_etf_1d_k_data,                Provider.Sina,      0, 0, "ETF Daily K-Data",         24,   RunMode.Async],
    [interface.get_stock_1d_k_data,              Provider.BaoStock,  0, 0, "Stock Daily K-Data",       24,   RunMode.Sync],
    [interface.get_stock_1w_k_data,              Provider.BaoStock,  0, 0, "Stock Weekly K-Data",      24,   RunMode.Sync],
    [interface.get_stock_1mon_k_data,            Provider.BaoStock,  0, 0, "Stock Monthly K-Data",     24,   RunMode.Sync],
    [interface.get_stock_1h_k_data,              Provider.BaoStock,  0, 0, "Stock 1 hours K-Data",     24,   RunMode.Sync],
    [interface.get_stock_30m_k_data,             Provider.BaoStock,  0, 0, "Stock 30 mins K-Data",     24,   RunMode.Sync],
    [interface.get_stock_15m_k_data,             Provider.BaoStock,  0, 3, "Stock 15 mins K-Data",     24,   RunMode.Sync],
    [interface.get_stock_5m_k_data,              Provider.BaoStock,  0, 3, "Stock 5 mins K-Data",      24,   RunMode.Sync],
    # [interface.get_stock_1m_k_data,              Provider.BaoStock,  0, 0, "Stock 1 mins K-Data",      24,   RunMode.Sync],

    # [interface.get_stock_1d_hfq_k_data,          Provider.BaoStock,  0, 0, "Stock Daily HFQ K-Data",   24,   RunMode.Sync],
    # [interface.get_stock_1w_hfq_k_data,          Provider.BaoStock,  0, 0, "Stock Weekly HFQ K-Data",  24,   RunMode.Sync],
    # [interface.get_stock_1mon_hfq_k_data,        Provider.BaoStock,  0, 0, "Stock Monthly HFQ K-Data", 24,   RunMode.Sync],
    # [interface.get_stock_1h_hfq_k_data,          Provider.BaoStock,  0, 0, "Stock 1 hours HFQ K-Data", 24,   RunMode.Sync],
    # [interface.get_stock_30m_hfq_k_data,         Provider.BaoStock,  0, 0, "Stock 30 mins K-Data",     24,   RunMode.Sync],
    # [interface.get_stock_15m_hfq_k_data,         Provider.BaoStock,  0, 0, "Stock 15 mins HFQ K-Data", 24,   RunMode.Sync],
    # [interface.get_stock_5m_hfq_k_data,          Provider.BaoStock,  0, 0, "Stock 5 mins HFQ K-Data",  24,   RunMode.Sync],
    # [interface.get_stock_1m_hfq_k_data,          Provider.BaoStock,  0, 0, "Stock 1 mins HFQ K-Data",  24,   RunMode.Sync],
]

data_set_us = [
    [interface.get_stock_list_data,              Provider.Yahoo,     0, 1, "Stock List",               24*6, RunMode.Sync],
    [interface.get_stock_trade_day,              Provider.Yahoo,     0, 1, "Trade Day",                24,   RunMode.Sync],

    [interface.get_stock_1d_k_data,              Provider.Yahoo,     0, 0, "Stock Daily K-Data",       24,   RunMode.Sync],
    [interface.get_stock_1w_k_data,              Provider.Yahoo,     0, 0, "Stock Weekly K-Data",      24,   RunMode.Sync],
    [interface.get_stock_1mon_k_data,            Provider.Yahoo,     0, 0, "Stock Monthly K-Data",     24,   RunMode.Sync],
    [interface.get_stock_1h_k_data,              Provider.Yahoo,     0, 0, "Stock 1 hours K-Data",     24,   RunMode.Sync],
    # [interface.get_stock_30m_k_data,             Provider.Yahoo,     0, 0, "Stock 30 mins K-Data",     24,   RunMode.Sync],
    [interface.get_stock_15m_k_data,             Provider.Yahoo,     0, 0, "Stock 15 mins K-Data",     24,   RunMode.Sync],
    [interface.get_stock_5m_k_data,              Provider.Yahoo,     0, 0, "Stock 5 mins K-Data",      24,   RunMode.Sync],
    [interface.get_stock_1m_k_data,              Provider.Yahoo,     0, 0, "Stock 1 mins K-Data",      24,   RunMode.Sync],
]


def loop_data_set(args):
    step1 = time.time()
    region, arg = args
    logger.info("Start Func: {}".format(arg[Para.FunName.value].__name__))

    arg[Para.FunName.value](region, arg[Para.Provider.value], arg[Para.Sleep.value], arg[Para.Process.value], arg[Para.Desc.value], arg[Para.Mode.value])
    logger.info("End Func: {}, cost: {}\n".format(arg[Para.FunName.value].__name__, time.time() - step1))

    return arg[Para.FunName.value].__name__

    # try:
    #     arg[Para.FunName.value](region, arg[Para.Provider.value], arg[Para.Sleep.value], arg[Para.Process.value], arg[Para.Desc.value], arg[Para.Mode.value])
    #     logger.info("End Func: {}, cost: {}\n".format(arg[Para.FunName.value].__name__, time.time() - step1))
    #     return arg[Para.FunName.value].__name__
    # except Exception as e:
    #     logger.error("End Func: {}, cost: {}, errors: {}\n".format(arg[Para.FunName.value].__name__, time.time() - step1, e))
    # return None


def fetch_data(region: Region):
    print("")
    print("*" * 80)
    print("*    Start Fetching {} Stock information...      {}".format(region.value.upper(), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
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

    cache = get_cache()
    data_set = [item for item in data_set if not valid(region, item[0].__name__, item[Para.Cache.value], cache)]

    with tqdm(total=len(data_set), ncols=80, desc="total", position=0, leave=True) as pbar:
        for item in data_set:
            result = loop_data_set([region, item])
            if result is not None:
                dump_cache(region, result, cache)
            pbar.update()
