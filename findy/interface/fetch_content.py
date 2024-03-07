import warnings
warnings.filterwarnings("ignore")

import logging
import os

from findy.interface import Region, Provider
from findy.task import RunMode

logger = logging.getLogger(__name__)


class task():
    @staticmethod
    async def get_stock_list_data(args):
        # 股票列表
        from findy.database.schema.meta.stock_meta import Stock
        await Stock.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_fund_list_data(args):
        # 基金列表
        from findy.database.schema.meta.fund_meta import Fund
        await Fund.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_trade_day(args):
        # 交易日
        from findy.database.schema.quotes.trade_day import StockTradeDay
        await StockTradeDay.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_main_index(args):
        from findy.database.schema.meta.stock_meta import Index
        await Index.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_summary_data(args):
        # 市场整体估值
        from findy.database.schema.misc.overall import StockSummary
        await StockSummary.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_detail_data(args):
        # 个股详情
        from findy.database.schema.meta.stock_meta import StockDetail
        await StockDetail.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_finance_factor_data(args):
        # 主要财务指标
        from findy.database.schema.fundamental.finance import FinanceFactor
        await FinanceFactor.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_balance_sheet_data(args):
        # 资产负债表
        from findy.database.schema.fundamental.finance import BalanceSheet
        await BalanceSheet.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_income_statement_data(args):
        # 收益表
        from findy.database.schema.fundamental.finance import IncomeStatement
        await IncomeStatement.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_cashflow_statement_data(args):
        # 现金流量表
        from findy.database.schema.fundamental.finance import CashFlowStatement
        await CashFlowStatement.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_moneyflow_data(args):
        # 股票资金流向表
        from findy.database.schema.misc.money_flow import StockMoneyFlow
        await StockMoneyFlow.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_dividend_financing_data(args):
        # 除权概览表
        from findy.database.schema.fundamental.dividend_financing import DividendFinancing
        await DividendFinancing.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_dividend_detail_data(args):
        # 除权具细表
        from findy.database.schema.fundamental.dividend_financing import DividendDetail
        await DividendDetail.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_rights_issue_detail_data(args):
        # 配股表
        from findy.database.schema.fundamental.dividend_financing import RightsIssueDetail
        await RightsIssueDetail.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_spo_detail_data(args):
        # 现金增资
        from findy.database.schema.fundamental.dividend_financing import SpoDetail
        await SpoDetail.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_margin_trading_summary_data(args):
        # 融资融券概况
        from findy.database.schema.misc.overall import MarginTradingSummary
        await MarginTradingSummary.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_cross_market_summary_data(args):
        # 北向/南向成交概况
        from findy.database.schema.misc.overall import CrossMarketSummary
        await CrossMarketSummary.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_holder_trading_data(args):
        # 股东交易
        from findy.database.schema.fundamental.trading import HolderTrading
        await HolderTrading.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_top_ten_holder_data(args):
        # 前十股东表
        from findy.database.schema.misc.holder import TopTenHolder
        await TopTenHolder.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_top_ten_tradable_holder_data(args):
        # 前十可交易股东表
        from findy.database.schema.misc.holder import TopTenTradableHolder
        await TopTenTradableHolder.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_valuation_data(args):
        # 个股估值数据
        from findy.database.schema.fundamental.valuation import StockValuation
        await StockValuation.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_etf_valuation_data(args):
        # ETF估值数据
        from findy.database.schema.fundamental.valuation import EtfValuation
        await EtfValuation.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1d_k_data(args):
        # 日线
        from findy.database.schema.quotes.stock.stock_1d_kdata import Stock1dKdata
        await Stock1dKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1d_hfq_k_data(args):
        # 日线复权
        from findy.database.schema.quotes.stock.stock_1d_kdata import Stock1dHfqKdata
        await Stock1dHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1w_k_data(args):
        # 周线
        from findy.database.schema.quotes.stock.stock_1wk_kdata import Stock1wkKdata
        await Stock1wkKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1w_hfq_k_data(args):
        # 周线复权
        from findy.database.schema.quotes.stock.stock_1wk_kdata import Stock1wkHfqKdata
        await Stock1wkHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1mon_k_data(args):
        # 月线
        from findy.database.schema.quotes.stock.stock_1mon_kdata import Stock1monKdata
        await Stock1monKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1mon_hfq_k_data(args):
        # 月线复权
        from findy.database.schema.quotes.stock.stock_1mon_kdata import Stock1monHfqKdata
        await Stock1monHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1m_k_data(args):
        # 1分钟线
        from findy.database.schema.quotes.stock.stock_1m_kdata import Stock1mKdata
        await Stock1mKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1m_hfq_k_data(args):
        # 1分钟线复权
        from findy.database.schema.quotes.stock.stock_1m_kdata import Stock1mHfqKdata
        await Stock1mHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_5m_k_data(args):
        # 5分钟线
        from findy.database.schema.quotes.stock.stock_5m_kdata import Stock5mKdata
        await Stock5mKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_5m_hfq_k_data(args):
        # 5分钟线复权
        from findy.database.schema.quotes.stock.stock_5m_kdata import Stock5mHfqKdata
        await Stock5mHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_15m_k_data(args):
        # 15分钟线
        from findy.database.schema.quotes.stock.stock_15m_kdata import Stock15mKdata
        await Stock15mKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_15m_hfq_k_data(args):
        # 15分钟线复权
        from findy.database.schema.quotes.stock.stock_15m_kdata import Stock15mHfqKdata
        await Stock15mHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_30m_k_data(args):
        # 30分钟线
        from findy.database.schema.quotes.stock.stock_30m_kdata import Stock30mKdata
        await Stock30mKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_30m_hfq_k_data(args):
        # 30分钟线复权
        from findy.database.schema.quotes.stock.stock_30m_kdata import Stock30mHfqKdata
        await Stock30mHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1h_k_data(args):
        # 1小时线
        from findy.database.schema.quotes.stock.stock_1h_kdata import Stock1hKdata
        await Stock1hKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_stock_1h_hfq_k_data(args):
        # 1小时线复权
        from findy.database.schema.quotes.stock.stock_1h_kdata import Stock1hHfqKdata
        await Stock1hHfqKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_etf_1d_k_data(args):
        from findy.database.schema.quotes.etf.etf_1d_kdata import Etf1dKdata
        await Etf1dKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_index_1d_k_data(args):
        from findy.database.schema.quotes.index.index_1d_kdata import Index1dKdata
        await Index1dKdata.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_news_title(args):
        from findy.database.schema.meta.news_meta import NewsTitle
        await NewsTitle.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])

    @staticmethod
    async def get_news_content(args):
        from findy.database.schema.meta.news_meta import NewsContent
        await NewsContent.record_data(args[0], args[1], sleep_time=args[2], share_para=args[3:])
        
    @staticmethod
    async def get_news(args):
        from findy.database.schema.meta.news_meta import News
        await News.record_data(args[0], args[1], sleep_time=args[2])
    

task_stock_chn = [
    ["chn_stock_tick", "task_001", RunMode.Serial,   24 * 6,  task.get_stock_list_data,              [Region.CHN, Provider.Exchange,  0, os.cpu_count(), 10, "Stock List"]],
    ["chn_stock_tick", "task_002", RunMode.Serial,   24,      task.get_stock_trade_day,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Trade Day"]],
#   ["chn_stock_tick", "task_003", RunMode.Serial,   24 * 6,  task.get_fund_list_data,               [Region.CHN, Provider.Exchange,  0, os.cpu_count(), 10, "Fund List"]],
    ["chn_stock_tick", "task_004", RunMode.Serial,   24,      task.get_stock_main_index,             [Region.CHN, Provider.Exchange,  0, os.cpu_count(), 10, "Main Index"]],
    ["chn_stock_tick", "task_005", RunMode.Parallel, 24 * 6,  task.get_stock_detail_data,            [Region.CHN, Provider.TuShare,   0, os.cpu_count(),  4, "Stock Detail"]],

#   ["chn_stock_tick", "task_006", RunMode.Parallel, 24 * 6,  task.get_dividend_financing_data,      [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Divdend Financing"]],
#   ["chn_stock_tick", "task_007", RunMode.Parallel, 24 * 6,  task.get_top_ten_holder_data,          [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Top Ten Holder"]],
#   ["chn_stock_tick", "task_008", RunMode.Parallel, 24 * 6,  task.get_top_ten_tradable_holder_data, [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Top Ten Tradable Holder"]],
#   ["chn_stock_tick", "task_009", RunMode.Parallel, 24 * 6,  task.get_dividend_detail_data,         [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Divdend Detail"]],
#   ["chn_stock_tick", "task_010", RunMode.Parallel, 24 * 6,  task.get_spo_detail_data,              [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "SPO Detail"]],
#   ["chn_stock_tick", "task_011", RunMode.Parallel, 24 * 6,  task.get_rights_issue_detail_data,     [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Rights Issue Detail"]],
#   ["chn_stock_tick", "task_012", RunMode.Parallel, 24 * 6,  task.get_holder_trading_data,          [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Holder Trading"]],

    # below functions call join-quant sdk task which limit at most 3 concurrent request
#   ["chn_stock_tick", "task_013", RunMode.Parallel, 24 * 6,  task.get_finance_factor_data,          [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Finance Factor"]],
#   ["chn_stock_tick", "task_014", RunMode.Parallel, 24 * 6,  task.get_balance_sheet_data,           [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Balance Sheet"]],
#   ["chn_stock_tick", "task_015", RunMode.Parallel, 24 * 6,  task.get_income_statement_data,        [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "Income Statement"]],
#   ["chn_stock_tick", "task_016", RunMode.Parallel, 24 * 6,  task.get_cashflow_statement_data,      [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "CashFlow Statement"]],
#   ["chn_stock_tick", "task_017", RunMode.Parallel, 24 * 6,  task.get_stock_valuation_data,         [Region.CHN, Provider.JoinQuant, 0, os.cpu_count(), 10, "Stock Valuation"]],
#   ["chn_stock_tick", "task_018", RunMode.Parallel, 24 * 6,  task.get_cross_market_summary_data,    [Region.CHN, Provider.JoinQuant, 0, os.cpu_count(), 10, "Cross Market Summary"]],
#   ["chn_stock_tick", "task_019", RunMode.Parallel, 24 * 6,  task.get_stock_summary_data,           [Region.CHN, Provider.Exchange,  0, os.cpu_count(), 10, "Stock Summary"]],
#   ["chn_stock_tick", "task_020", RunMode.Parallel, 24 * 6,  task.get_margin_trading_summary_data,  [Region.CHN, Provider.JoinQuant, 0, os.cpu_count(), 10, "Margin Trading Summary"]],
#   ["chn_stock_tick", "task_021", RunMode.Parallel, 24 * 6,  task.get_etf_valuation_data,           [Region.CHN, Provider.JoinQuant, 0, os.cpu_count(), 10, "ETF Valuation"]],
#   ["chn_stock_tick", "task_022", RunMode.Parallel, 24 * 6,  task.get_moneyflow_data,               [Region.CHN, Provider.Sina,      0, os.cpu_count(), 10, "MoneyFlow Statement"]],

#   ["chn_stock_tick", "task_023", RunMode.Parallel, 24,      task.get_etf_1d_k_data,                [Region.CHN, Provider.Sina,      0, os.cpu_count(), 10, "ETF Daily K-Data"]],
    ["chn_stock_tick", "task_024", RunMode.Parallel, 24,      task.get_stock_1d_k_data,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 99, "Stock Daily K-Data"]],
    ["chn_stock_tick", "task_025", RunMode.Parallel, 24,      task.get_stock_1w_k_data,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 99, "Stock Weekly K-Data"]],
    ["chn_stock_tick", "task_026", RunMode.Parallel, 24,      task.get_stock_1mon_k_data,            [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 99, "Stock Monthly K-Data"]],
    ["chn_stock_tick", "task_027", RunMode.Parallel, 24,      task.get_stock_1h_k_data,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 50, "Stock 1 hours K-Data"]],
    ["chn_stock_tick", "task_028", RunMode.Parallel, 24,      task.get_stock_30m_k_data,             [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 50, "Stock 30 mins K-Data"]],
    ["chn_stock_tick", "task_029", RunMode.Parallel, 24,      task.get_stock_15m_k_data,             [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 40, "Stock 15 mins K-Data"]],
    ["chn_stock_tick", "task_030", RunMode.Parallel, 24,      task.get_stock_5m_k_data,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 20, "Stock 5 mins K-Data"]],
#   ["chn_stock_tick", "task_031", RunMode.Parallel, 24,      task.get_stock_1m_k_data,              [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 1 mins K-Data"]],

#   ["chn_stock_tick", "task_032", RunMode.Parallel, 24,      task.get_stock_1d_hfq_k_data,          [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock Daily HFQ K-Data"]],
#   ["chn_stock_tick", "task_033", RunMode.Parallel, 24,      task.get_stock_1w_hfq_k_data,          [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock Weekly HFQ K-Data"]],
#   ["chn_stock_tick", "task_034", RunMode.Parallel, 24,      task.get_stock_1mon_hfq_k_data,        [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock Monthly HFQ K-Data"]],
#   ["chn_stock_tick", "task_035", RunMode.Parallel, 24,      task.get_stock_1h_hfq_k_data,          [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 1 hours HFQ K-Data"]],
#   ["chn_stock_tick", "task_036", RunMode.Parallel, 24,      task.get_stock_30m_hfq_k_data,         [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 30 mins HFQ K-Data"]],
#   ["chn_stock_tick", "task_037", RunMode.Parallel, 24,      task.get_stock_15m_hfq_k_data,         [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 15 mins HFQ K-Data"]],
#   ["chn_stock_tick", "task_038", RunMode.Parallel, 24,      task.get_stock_5m_hfq_k_data,          [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 5 mins HFQ K-Data"]],
#   ["chn_stock_tick", "task_039", RunMode.Parallel, 24,      task.get_stock_1m_hfq_k_data,          [Region.CHN, Provider.BaoStock,  0, os.cpu_count(), 10, "Stock 1 mins HFQ K-Data"]],
]


task_news_chn = [
    # ["chn_news",      "task_001", RunMode.Parallel,  24,      task.get_news_title,                   [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 10, "News Title"]],
    ["chn_news",      "task_002", RunMode.Parallel,  24,      task.get_news_content,                 [Region.CHN, Provider.EastMoney, 0, os.cpu_count(), 99, "News Content"]],
]


task_stock_us = [
    ["us_stock_tick", "task_001", RunMode.Serial,    24 * 6,  task.get_stock_list_data,              [Region.US,  Provider.Exchange,  0, os.cpu_count(),  3, "Stock List"]],
    ["us_stock_tick", "task_002", RunMode.Serial,    24 * 6,  task.get_stock_trade_day,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Trade Day"]],
    ["us_stock_tick", "task_003", RunMode.Serial,    24 * 6,  task.get_stock_main_index,             [Region.US,  Provider.Exchange,  0, os.cpu_count(),  3, "Main Index"]],
    ["us_stock_tick", "task_004", RunMode.Parallel,  24 * 6,  task.get_stock_detail_data,            [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock Detail"]],

    ["us_stock_tick", "task_005", RunMode.Parallel,  24 * 6,  task.get_index_1d_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Index Daily K-Data"]],
    ["us_stock_tick", "task_006", RunMode.Parallel,  24 * 6,  task.get_stock_1d_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock Daily K-Data"]],
    ["us_stock_tick", "task_007", RunMode.Parallel,  24 * 6,  task.get_stock_1w_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock Weekly K-Data"]],
    ["us_stock_tick", "task_008", RunMode.Parallel,  24 * 6,  task.get_stock_1mon_k_data,            [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock Monthly K-Data"]],
    ["us_stock_tick", "task_009", RunMode.Parallel,  24 * 6,  task.get_stock_1h_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock 1 hours K-Data"]],
    ["us_stock_tick", "task_010", RunMode.Parallel,  24 * 6,  task.get_stock_30m_k_data,             [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock 30 mins K-Data"]],
    ["us_stock_tick", "task_011", RunMode.Parallel,  24 * 6,  task.get_stock_15m_k_data,             [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock 15 mins K-Data"]],
    ["us_stock_tick", "task_012", RunMode.Parallel,  24 * 6,  task.get_stock_5m_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock 5 mins K-Data"]],
    ["us_stock_tick", "task_013", RunMode.Parallel,  24 * 6,  task.get_stock_1m_k_data,              [Region.US,  Provider.Yahoo,     0, os.cpu_count(),  3, "Stock 1 mins K-Data"]],
]


task_news_us = [
    ["us_news",      "task_001", RunMode.Serial,     24,      task.get_news,                         [Region.US, Provider.NewsData,   0, os.cpu_count(), 99, "News"]],
]
