# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.database.schema.fundamental.finance import CashFlowStatement
from findy.database.plugins.eastmoney.common import to_report_period_type
from findy.database.plugins.eastmoney.finance.base_china_stock_finance_recorder import BaseChinaStockFinanceRecorder
from findy.utils.convert import to_float

cash_flow_map = {
    # 经营活动产生的现金流量
    #
    # 销售商品、提供劳务收到的现金
    "Salegoodsservicerec": "cash_from_selling",

    # 收到的税费返还
    "Taxreturnrec": "tax_refund",

    # 收到其他与经营活动有关的现金
    "Otheroperaterec": "cash_from_other_op",

    # 经营活动现金流入小计
    "Sumoperateflowin": "total_op_cash_inflows",

    # 购买商品、接受劳务支付的现金
    "Buygoodsservicepay": "cash_to_goods_services",
    # 支付给职工以及为职工支付的现金
    "Employeepay": "cash_to_employees",
    # 支付的各项税费
    "Taxpay": "taxes_and_surcharges",
    # 支付其他与经营活动有关的现金
    "Otheroperatepay": "cash_to_other_related_op",
    # 经营活动现金流出小计
    "Sumoperateflowout": "total_op_cash_outflows",

    # 经营活动产生的现金流量净额
    "Netoperatecashflow": "net_op_cash_flows",

    # 投资活动产生的现金流量

    # 收回投资收到的现金
    "Disposalinvrec": "cash_from_disposal_of_investments",
    # 取得投资收益收到的现金
    "Invincomerec": "cash_from_returns_on_investments",
    # 处置固定资产、无形资产和其他长期资产收回的现金净额
    "Dispfilassetrec": "cash_from_disposal_fixed_intangible_assets",
    # 处置子公司及其他营业单位收到的现金净额
    "Dispsubsidiaryrec": "cash_from_disposal_subsidiaries",

    # 收到其他与投资活动有关的现金
    "Otherinvrec": "cash_from_other_investing",

    # 投资活动现金流入小计
    "Suminvflowin": "total_investing_cash_inflows",

    # 购建固定资产、无形资产和其他长期资产支付的现金
    "Buyfilassetpay": "cash_to_acquire_fixed_intangible_assets",
    # 投资支付的现金
    "Invpay": "cash_to_investments",

    # 取得子公司及其他营业单位支付的现金净额
    "Getsubsidiarypay": "cash_to_acquire_subsidiaries",

    # 支付其他与投资活动有关的现金
    "Otherinvpay": "cash_to_other_investing",

    # 投资活动现金流出小计
    "Suminvflowout": "total_investing_cash_outflows",

    # 投资活动产生的现金流量净额
    "Netinvcashflow": "net_investing_cash_flows",

    # 筹资活动产生的现金流量
    #
    # 吸收投资收到的现金
    "Acceptinvrec": "cash_from_accepting_investment",
    # 子公司吸收少数股东投资收到的现金
    "Subsidiaryaccept": "cash_from_subsidiaries_accepting_minority_interest",

    # 取得借款收到的现金
    "Loanrec": "cash_from_borrowings",
    # 发行债券收到的现金
    "Issuebondrec": "cash_from_issuing_bonds",
    # 收到其他与筹资活动有关的现金
    "Otherfinarec": "cash_from_other_financing",

    # 筹资活动现金流入小计
    "Sumfinaflowin": "total_financing_cash_inflows",

    # 偿还债务支付的现金
    "Repaydebtpay": "cash_to_repay_borrowings",

    # 分配股利、利润或偿付利息支付的现金
    "Diviprofitorintpay": "cash_to_pay_interest_dividend",

    # 子公司支付给少数股东的股利、利润
    "Subsidiarypay": "cash_to_pay_subsidiaries_minority_interest",

    # 支付其他与筹资活动有关的现金
    "Otherfinapay": "cash_to_other_financing",
    # 筹资活动现金流出小计
    "Sumfinaflowout": "total_financing_cash_outflows",

    # 筹资活动产生的现金流量净额
    "Netfinacashflow": "net_financing_cash_flows",
    # 汇率变动对现金及现金等价物的影响
    "Effectexchangerate": "foreign_exchange_rate_effect",
    # 现金及现金等价物净增加额
    "Nicashequi": "net_cash_increase",
    # 加: 期初现金及现金等价物余额
    "Cashequibeginning": "cash_at_beginning",
    # 期末现金及现金等价物余额
    "Cashequiending": "cash",

    # 银行相关
    # 客户存款和同业及其他金融机构存放款项净增加额
    "Nideposit": "fi_deposit_increase",
    # 向中央银行借款净增加额
    "Niborrowfromcbank": "fi_borrow_from_central_bank_increase",
    # 存放中央银行和同业款项及其他金融机构净减少额
    "Nddepositincbankfi": "fi_deposit_in_others_decrease",
    # 拆入资金及卖出回购金融资产款净增加额
    "Niborrowsellbuyback": "fi_borrowing_and_sell_repurchase_increase",
    # 其中:卖出回购金融资产款净增加额
    "Nisellbuybackfasset": "fi_sell_repurchase_increase",
    # 拆出资金及买入返售金融资产净减少额
    "Ndlendbuysellback": "fi_lending_and_buy_repurchase_decrease",
    # 其中:拆出资金净减少额
    "Ndlendfund": "fi_lending_decrease",
    # 买入返售金融资产净减少额
    "Ndbuysellbackfasset": "fi_buy_repurchase_decrease",
    # 收取的利息、手续费及佣金的现金
    "Intandcommrec": "fi_cash_from_interest_commission",
    # 客户贷款及垫款净增加额
    "Niloanadvances": "fi_loan_advance_increase",
    # 存放中央银行和同业及其他金融机构款项净增加额
    "Nidepositincbankfi": "fi_deposit_in_others_increase",
    # 拆出资金及买入返售金融资产净增加额
    "Nilendsellbuyback": "fi_lending_and_buy_repurchase_increase",
    # 其中:拆出资金净增加额
    "Nilendfund": "fi_lending_increase",
    # 拆入资金及卖出回购金融资产款净减少额
    "Ndborrowsellbuyback": "fi_borrowing_and_sell_repurchase_decrease",
    # 其中:拆入资金净减少额
    "Ndborrowfund": "fi_borrowing_decrease",
    # 卖出回购金融资产净减少额
    "Ndsellbuybackfasset": "fi_sell_repurchase_decrease",
    # 支付利息、手续费及佣金的现金
    "Intandcommpay": "fi_cash_to_interest_commission",
    # 应收账款净增加额
    "Niaccountrec": "fi_account_receivable_increase",
    # 偿付债券利息支付的现金
    "Bondintpay": "fi_cash_to_pay_interest",

    # 保险相关
    # 收到原保险合同保费取得的现金
    "Premiumrec": "fi_cash_from_premium_of_original",
    # 保户储金及投资款净增加额
    "Niinsureddepositinv": "fi_insured_deposit_increase",
    # 银行及证券业务卖出回购资金净增加额
    "Nisellbuyback": "fi_bank_broker_sell_repurchase_increase",
    # 银行及证券业务买入返售资金净减少额
    "Ndbuysellback": "fi_bank_broker_buy_repurchase_decrease",
    # 支付原保险合同赔付等款项的现金
    "Indemnitypay": "fi_cash_to_insurance_claim",
    # 支付再保险业务现金净额
    "Netripay": "fi_cash_to_reinsurance",
    # 银行业务及证券业务拆借资金净减少额
    "Ndlendfund": "fi_lending_decrease",
    # 银行业务及证券业务卖出回购资金净减少额
    "Ndsellbuyback": "fi_bank_broker_sell_repurchase_decrease",
    # 支付保单红利的现金
    "Divipay": "fi_cash_to_dividends",
    # 保户质押贷款净增加额
    "Niinsuredpledgeloan": "fi_insured_pledge_loans_increase",
    # 收购子公司及其他营业单位支付的现金净额
    "Buysubsidiarypay": "fi_cash_to_acquire_subsidiaries",
    # 处置子公司及其他营业单位流出的现金净额
    "Dispsubsidiarypay": "fi_cash_to_disposal_subsidiaries",
    # 支付卖出回购金融资产款现金净额
    "Netsellbuybackfassetpay": "fi_cash_to_sell_repurchase",

    # 券商相关
    # 拆入资金净增加额
    "Niborrowfund": "fi_borrowing_increase",
    # 代理买卖证券收到的现金净额
    "Agenttradesecurityrec": "fi_cash_from_trading_agent",
    # 回购业务资金净增加额
    "Nibuybackfund": "fi_cash_from_repurchase_increase",
    # 处置交易性金融资产的净减少额
    "Nddisptradefasset": "fi_disposal_trade_asset_decrease",
    # 回购业务资金净减少额
    "Ndbuybackfund": "fi_repurchase_decrease",
    # 代理买卖证券支付的现金净额（净减少额）
    "Agenttradesecuritypay": "fi_cash_to_agent_trade",
}


class ChinaStockCashFlowRecorder(BaseChinaStockFinanceRecorder):
    data_schema = CashFlowStatement

    url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetXianJinLiuLiangBiaoList'
    finance_report_type = 'XianJinLiuLiangBiaoList'
    data_type = 4

    def format(self, entity, df):
        cols = list(df.columns)
        str_cols = ['Title']
        date_cols = [self.get_original_time_field()]
        float_cols = list(set(cols) - set(str_cols) - set(date_cols))
        for column in float_cols:
            df[column] = df[column].apply(lambda x: to_float(x[0]))

        df.rename(columns=cash_flow_map, inplace=True)

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
