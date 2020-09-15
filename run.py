import warnings
warnings.filterwarnings("ignore")

import logging
import os
import time
from datetime import datetime, timedelta
from functools import wraps
import multiprocessing

from tqdm import tqdm
import pickle
from apscheduler.schedulers.background import BackgroundScheduler

from zvt import zvt_env
from zvt.contract.common import Region, Provider
from zvt.domain import Stock, Etf, StockTradeDay, StockSummary, StockDetail, FinanceFactor, \
                       BalanceSheet, IncomeStatement, CashFlowStatement, StockMoneyFlow, \
                       DividendFinancing, DividendDetail, RightsIssueDetail, SpoDetail, \
                       MarginTradingSummary, CrossMarketSummary, HolderTrading, TopTenHolder, \
                       TopTenTradableHolder, StockValuation, EtfStock, EtfValuation, Stock1dKdata, \
                       Stock1dHfqKdata, Stock1dHfqKdata, Stock1wkKdata, Stock1wkHfqKdata, \
                       Stock1monKdata, Stock1monHfqKdata, Stock1monHfqKdata, Stock1monHfqKdata, \
                       Stock1mKdata, Stock1mHfqKdata, Stock1mHfqKdata, Stock5mKdata, Stock5mHfqKdata, \
                       Stock15mKdata, Stock15mHfqKdata, Stock30mKdata, Stock30mHfqKdata, \
                       Stock1hKdata, Stock1hHfqKdata, Etf1dKdata

logger = logging.getLogger(__name__)
sched = BackgroundScheduler()


def get_cache():
    file = zvt_env['cache_path'] + '/' + 'cache.pkl'
    if os.path.exists(file) and os.path.getsize(file) > 0:
        with open(file, 'rb') as handle:
            return pickle.load(handle)
    return {}

def valid(region: Region, func_name, valid_time, data):
    key = "{}_{}".format(region.value, func_name)
    lasttime = data.get(key, None)
    if lasttime is not None:
        if lasttime > (datetime.now() - timedelta(hours=valid_time)):
            return True
    return False

def dump(region: Region, func_name, data):
    key = "{}_{}".format(region.value, func_name)
    file = zvt_env['cache_path'] + '/' + 'cache.pkl'
    with open(file, 'wb+') as handle:
        data.update({key : datetime.now()})
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


class interface():
    @staticmethod
    def get_stock_list_data(provider: Provider):
        # 股票列表
        Stock.record_data(provider=provider, sleeping_time=0)

    @staticmethod
    def get_etf_list(provider: Provider):
        Etf.record_data(provider=provider, sleeping_time=0)

    @staticmethod
    def get_stock_trade_day(provider: Provider, lock, region):
        # 交易日
        StockTradeDay.record_data(provider=provider, share_para=('Trade Day', 0, lock, True, region), sleeping_time=0)

    @staticmethod
    def get_stock_summary_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 市场整体估值
        StockSummary.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_detail_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 个股详情
        StockDetail.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_finance_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 主要财务指标
        FinanceFactor.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_balance_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 资产负债表
        BalanceSheet.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_income_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 收益表
        IncomeStatement.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_cashflow_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 现金流量表
        CashFlowStatement.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)
    
    @staticmethod
    def get_moneyflow_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 股票资金流向表
        StockMoneyFlow.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)
    
    @staticmethod
    def get_dividend_financing_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 除权概览表
        DividendFinancing.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_dividend_detail_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 除权具细表
        DividendDetail.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_rights_issue_detail_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 配股表
        RightsIssueDetail.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_spo_detail_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 现金增资
        SpoDetail.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_margin_trading_summary_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 融资融券概况
        MarginTradingSummary.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_cross_market_summary_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 北向/南向成交概况
        CrossMarketSummary.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_holder_trading_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 股东交易
        HolderTrading.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_top_ten_holder_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 前十股东表
        TopTenHolder.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_top_ten_tradable_holder_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 前十可交易股东表
        TopTenTradableHolder.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_valuation_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 个股估值数据
        StockValuation.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_etf_stock_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # ETF股票
        EtfStock.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_etf_valuation_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # ETF估值数据
        EtfValuation.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1d_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 日线
        Stock1dKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1d_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 日线复权
        Stock1dHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1w_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 周线
        Stock1wkKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1w_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 周线复权
        Stock1wkHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)
    
    @staticmethod
    def get_stock_1mon_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 月线
        Stock1monKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1mon_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 月线复权
        Stock1monHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1m_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 1分钟线
        Stock1mKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1m_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 1分钟线复权
        Stock1mHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_5m_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 5分钟线
        Stock5mKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_5m_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 5分钟线复权
        Stock5mHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_15m_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 15分钟线
        Stock15mKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_15m_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 15分钟线复权
        Stock15mHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_30m_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 30分钟线
        Stock30mKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_30m_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 30分钟线复权
        Stock30mHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1h_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 1小时线
        Stock1hKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_stock_1h_hfq_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        # 1小时线复权
        Stock1hHfqKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)

    @staticmethod
    def get_etf_1d_k_data(provider: Provider, sleep, desc, pc, lock, region, batch):
        Etf1dKdata.record_data(provider=provider, share_para=(desc, pc, lock, True, region), sleeping_time=sleep, batch_size=batch)


def init(l):
    global lock
    lock = l

def mp_tqdm(func, lock, region, shared=[], args=[], pc=4, reset=False):
    with multiprocessing.Pool(pc, initializer=init, initargs=(lock,), maxtasksperchild = 1 if reset else None) as p:
        data = get_cache()
        # args = [arg for arg in args if not valid(shared[0], arg[0].__name__, arg[4], data)]
        # The master process tqdm bar is at Position 0
        with tqdm(total=len(args), ncols=80, desc="total", leave=True) as pbar:
            for func_name in p.imap_unordered(func, [[pc, shared, arg] for arg in args], chunksize=1):
                lock.acquire()
                pbar.update()
                if func_name is not None: dump(shared[0], func_name, data)
                lock.release()
                time.sleep(1)

def run(args):
    pc, shared, data_set = args
    try:
        data_set[0](data_set[1], data_set[2], data_set[3], pc, lock, shared[0], shared[1])
        return data_set[0].__name__
    except Exception as e:
        print(e)
    return None


data_set_chn = [
    # [interface.get_dividend_financing_data,      Provider.EastMoney, 0, "Divdend Financing",        24*6],
    # [interface.get_top_ten_holder_data,          Provider.EastMoney, 0, "Top Ten Holder",           24*6],
    # [interface.get_finance_data,                 Provider.EastMoney, 0, "Finance Factor",           24*6],
    # [interface.get_balance_data,                 Provider.EastMoney, 0, "Balance Sheet",            24*6],
    # [interface.get_top_ten_tradable_holder_data, Provider.EastMoney, 0, "Top Ten Tradable Holder",  24*6],
    # [interface.get_income_data,                  Provider.EastMoney, 0, "Income Statement",         24*6],
    # [interface.get_moneyflow_data,               Provider.Sina,      1, "MoneyFlow Statement",      24],
    # [interface.get_dividend_detail_data,         Provider.EastMoney, 0, "Divdend Detail",           24*6],
    # [interface.get_spo_detail_data,              Provider.EastMoney, 0, "SPO Detail",               24*6],
    # [interface.get_rights_issue_detail_data,     Provider.EastMoney, 0, "Rights Issue Detail",      24],
    # [interface.get_holder_trading_data,          Provider.EastMoney, 0, "Holder Trading",           24],
    # [interface.get_etf_valuation_data,           Provider.JoinQuant, 0, "ETF Valuation",            24],
    # [interface.get_stock_summary_data,           Provider.JoinQuant, 0, "Stock Summary",            24],  
    # [interface.get_stock_detail_data,            Provider.EastMoney, 0, "Stock Detail",             24], 
    # [interface.get_cashflow_data,                Provider.EastMoney, 0, "CashFlow Statement",       24],
    # [interface.get_stock_valuation_data,         Provider.JoinQuant, 0, "Stock Valuation",          24],
    # [interface.get_etf_stock_data,               Provider.JoinQuant, 0, "ETF Stock",                24],
    # [interface.get_margin_trading_summary_data,  Provider.JoinQuant, 0, "Margin Trading Summary",   24],
    # [interface.get_cross_market_summary_data,    Provider.JoinQuant, 0, "Cross Market Summary",     24],

    [interface.get_stock_1d_k_data,              Provider.JoinQuant, 0, "Stock Daily K-Data",       24], 
    # [interface.get_stock_1d_hfq_k_data,          Provider.JoinQuant, 0, "Stock Daily HFQ K-Data",   24],
    # [interface.get_stock_1w_k_data,              Provider.JoinQuant, 0, "Stock Weekly K-Data",      24],
    # [interface.get_stock_1w_hfq_k_data,          Provider.JoinQuant, 0, "Stock Weekly HFQ K-Data",  24],
    # [interface.get_stock_1mon_k_data,            Provider.JoinQuant, 0, "Stock Monthly K-Data",     24], 
    # [interface.get_etf_1d_k_data,                Provider.Sina,      1, "ETF Daily K-Data",         24],

    # [interface.get_stock_1mon_hfq_k_data,        Provider.JoinQuant, 0, "Stock Monthly HFQ K-Data", 24],
    # [interface.get_stock_1h_k_data,              Provider.JoinQuant, 0, "Stock 1 hours K-Data",     24], 
    # [interface.get_stock_1h_hfq_k_data,          Provider.JoinQuant, 0, "Stock 1 hours HFQ K-Data", 24],
    # [interface.get_stock_30m_k_data,             Provider.JoinQuant, 0, "Stock 30 mins K-Data",     24], 
    # [interface.get_stock_30m_hfq_k_data,         Provider.JoinQuant, 0, "Stock 30 mins K-Data",     24],
    # [interface.get_stock_1m_k_data,              Provider.JoinQuant, 0, "Stock 1 mins K-Data",      24], 
    # [interface.get_stock_1m_hfq_k_data,          Provider.JoinQuant, 0, "Stock 1 mins HFQ K-Data",  24],
    # [interface.get_stock_5m_k_data,              Provider.JoinQuant, 0, "Stock 5 mins K-Data",      24], 
    # [interface.get_stock_5m_hfq_k_data,          Provider.JoinQuant, 0, "Stock 5 mins HFQ K-Data",  24],
    # [interface.get_stock_15m_k_data,             Provider.JoinQuant, 0, "Stock 15 mins K-Data",     24], 
    # [interface.get_stock_15m_hfq_k_data,         Provider.JoinQuant, 0, "Stock 15 mins HFQ K-Data", 24],
]

data_set_us = [
    [interface.get_stock_1d_k_data,         Provider.Yahoo, 0, "Stock Daily K-Data",       24],
    [interface.get_stock_1w_k_data,         Provider.Yahoo, 0, "Stock Weekly K-Data",      24],
    [interface.get_stock_1mon_k_data,       Provider.Yahoo, 0, "Stock Monthly K-Data",     24], 
    # [interface.get_stock_1h_k_data,         Provider.Yahoo, 0, "Stock 1 hours K-Data",     24], 
    # [interface.get_stock_30m_k_data,        Provider.Yahoo, 0, "Stock 30 mins K-Data",     24], 
    # [interface.get_stock_15m_k_data,        Provider.Yahoo, 0, "Stock 15 mins K-Data",     24], 
    # [interface.get_stock_5m_k_data,         Provider.Yahoo, 0, "Stock 5 mins K-Data",      24], 
    # [interface.get_stock_1m_k_data,         Provider.Yahoo, 0, "Stock 1 mins K-Data",      24],
]

def fetch_data(lock, region: Region):
    print("")
    print("*"*80)
    print("*    Start Fetching {} Stock information...      {}".format(region.value.upper(), datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print("*"*80)

    if region == Region.CHN:
        data_set = data_set_chn
        # interface.get_stock_list_data(Provider.JoinQuant)
        # interface.get_etf_list(Provider.JoinQuant)
        interface.get_stock_trade_day(Provider.JoinQuant, lock, region)

    elif region == Region.US:
        data_set = data_set_us
        # interface.get_stock_list_data(Provider.Yahoo)
        interface.get_stock_trade_day(Provider.Yahoo, lock, region)

    else:
        data_set = []

    print("")
    print("parallel processing...")
    print("")

    batch_size = 50

    mp_tqdm(run, lock, region, shared=[region, batch_size], args=data_set, pc=3, reset=True)

    print("")
    print("\/"*40)


@sched.scheduled_job('interval', days=1, next_run_time=datetime.now())
def main():
    multiprocessing.freeze_support()
    l = multiprocessing.Lock()

    fetch_data(l, Region.CHN)
    fetch_data(l, Region.US)

    l.release


if __name__ == '__main__':
    main()

    sched.start()
    sched._thread.join()

