# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.database.schema.fundamental.finance import FinanceFactor
from findy.database.plugins.eastmoney.common import to_report_period_type
from findy.database.plugins.eastmoney.finance.base_china_stock_finance_recorder import BaseChinaStockFinanceRecorder
from findy.utils.convert import to_float

finance_factor_map = {
    # 基本每股收益(元)
    "Epsjb": "basic_eps",
    # 扣非每股收益(元)
    "Epskcjb": "deducted_eps",
    # 稀释每股收益(元)
    "Epsxs": "diluted_eps",
    # 每股净资产(元)
    "Bps": "bps",
    # 每股资本公积(元)
    "Mgzbgj": "capital_reserve_ps",
    # 每股未分配利润(元)
    "Mgwfplr": "undistributed_profit_ps",
    # 每股经营现金流(元)
    "Mgjyxjje": "op_cash_flow_ps",
    # 成长能力指标
    #
    # 营业总收入(元)
    "Totalincome": "total_op_income",
    # 毛利润(元)
    "Grossprofit": "gross_profit",
    # 归属净利润(元)
    "Parentnetprofit": "net_profit",
    # 扣非净利润(元)
    "Bucklenetprofit": "deducted_net_profit",
    # 营业总收入同比增长
    "Totalincomeyoy": "op_income_growth_yoy",
    # 归属净利润同比增长
    "Parentnetprofityoy": "net_profit_growth_yoy ",
    # 扣非净利润同比增长
    "Bucklenetprofityoy": "deducted_net_profit_growth_yoy",
    # 营业总收入滚动环比增长
    "Totalincomerelativeratio": "op_income_growth_qoq",
    # 归属净利润滚动环比增长
    "Parentnetprofitrelativeratio": "net_profit_growth_qoq",
    # 扣非净利润滚动环比增长
    "Bucklenetprofitrelativeratio": "deducted_net_profit_growth_qoq",
    # 盈利能力指标
    #
    # 净资产收益率(加权)
    "Roejq": "roe",
    # 净资产收益率(扣非/加权)
    "Roekcjq": "deducted_roe",
    # 总资产收益率(加权)
    "Allcapitalearningsrate": "rota",
    # 毛利率
    "Grossmargin": "gross_profit_margin",
    # 净利率
    "Netinterest": "net_margin",
    # 收益质量指标
    #
    # 预收账款/营业收入
    "Accountsrate": "advance_receipts_per_op_income",
    # 销售净现金流/营业收入
    "Salesrate": "sales_net_cash_flow_per_op_income",
    # 经营净现金流/营业收入
    "Operatingrate": "op_net_cash_flow_per_op_income",
    # 实际税率
    "Taxrate": "actual_tax_rate",
    # 财务风险指标
    #
    # 流动比率
    "Liquidityratio": "current_ratio",
    # 速动比率
    "Quickratio": "quick_ratio",
    # 现金流量比率
    "Cashflowratio": "cash_flow_ratio",
    # 资产负债率
    "Assetliabilityratio": "debt_asset_ratio",
    # 权益乘数
    "Equitymultiplier": "em",
    # 产权比率
    "Equityratio": "equity_ratio",
    # 营运能力指标(一般企业)
    #
    # 总资产周转天数(天)
    "Totalassetsdays": "total_assets_turnover_days",
    # 存货周转天数(天)
    "Inventorydays": "inventory_turnover_days",
    # 应收账款周转天数(天)
    "Accountsreceivabledays": "receivables_turnover_days",
    # 总资产周转率(次)
    "Totalassetrate": "total_assets_turnover",
    # 存货周转率(次)
    "Inventoryrate": "inventory_turnover",
    # 应收账款周转率(次)
    "Accountsreceiveablerate": "receivables_turnover",

    # 专项指标(银行)
    #
    # 存款总额
    "Totaldeposit": "fi_total_deposit",
    # 贷款总额
    "Totalloan": "fi_total_loan",
    # 存贷款比例
    "Depositloanratio": "fi_loan_deposit_ratio",
    # 资本充足率
    "Capitaladequacyratio": "fi_capital_adequacy_ratio",
    # 核心资本充足率
    "Corecapitaladequacyratio": "fi_core_capital_adequacy_ratio",
    # 不良贷款率
    "Nplratio": "fi_npl_ratio",
    # 不良贷款拨备覆盖率
    "Nplprovisioncoverage": "fi_npl_provision_coverage",
    # 资本净额
    "Netcapital_b": "fi_net_capital",
    # 专项指标(保险)
    #
    # 总投资收益率
    "Tror": "insurance_roi",
    # 净投资收益率
    "Nror": "insurance_net_investment_yield",
    # 已赚保费
    "Eapre": "insurance_earned_premium",
    # 赔付支出
    "Comexpend": "insurance_payout",
    # 退保率
    "Surrate": "insurance_surrender_rate",
    # 偿付能力充足率
    "Solvenra": "insurance_solvency_adequacy_ratio",
    # 专项指标(券商)
    #
    # 净资本
    "Netcapital": "broker_net_capital",
    # 净资产
    "Netassets": "broker_net_assets",
    # 净资本/净资产
    "Captialrate": "broker_net_capital_assets_ratio",
    # 自营固定收益类证券规模/净资本
    "Incomesizerate": "broker_self_operated_fixed_income_securities_net_capital_ratio",
}


class ChinaStockFinanceFactorRecorder(BaseChinaStockFinanceRecorder):
    url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetZhuYaoZhiBiaoList'
    finance_report_type = 'ZhuYaoZhiBiaoList'

    data_schema = FinanceFactor
    data_type = 1

    def format(self, entity, df):
        cols = list(df.columns)
        str_cols = ['Title']
        date_cols = [self.get_original_time_field()]
        float_cols = list(set(cols) - set(str_cols) - set(date_cols))
        for column in float_cols:
            df[column] = df[column].apply(lambda x: to_float(x))

        df.rename(columns=finance_factor_map, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['report_period'] = df['timestamp'].apply(lambda x: to_report_period_type(x))
        df['report_date'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df
