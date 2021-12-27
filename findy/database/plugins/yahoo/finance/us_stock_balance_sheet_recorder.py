# -*- coding: utf-8 -*-
from datetime import datetime
import time

import pandas as pd
import numpy as np

from findy.interface import Region, Provider, EntityType
from findy.database.schema.fundamental.finance import BalanceSheet
from findy.database.plugins.recorder import TimestampsDataRecorder
from findy.database.plugins.yahoo.common import to_report_period_type
from findy.utils.pd import pd_valid
from findy.vendor.yfinance import YH


balance_sheet_map = {
    # 流动资产
    #
    # 货币资金
    "Monetaryfund": "cash_and_cash_equivalents",
    # 应收票据
    "Billrec": "note_receivable",
    # 应收账款
    "Accountrec": "accounts_receivable",
    # 预付款项
    "Advancepay": "advances_to_suppliers",
    # 其他应收款
    "Otherrec": "other_receivables",
    # 存货
    "Inventory": "inventories",
    # 一年内到期的非流动资产
    "Nonlassetoneyear": "current_portion_of_non_current_assets",
    # 其他流动资产
    "Otherlasset": "other_current_assets",
    # 流动资产合计
    "Sumlasset": "total_current_assets",
    # 非流动资产
    #
    # 可供出售金融资产
    "Saleablefasset": "fi_assets_saleable",
    # 长期应收款
    "Ltrec": "long_term_receivables",
    # 长期股权投资
    "Ltequityinv": "long_term_equity_investment",
    # 投资性房地产
    "Estateinvest": "real_estate_investment",
    # 固定资产
    "Fixedasset": "fixed_assets",
    # 在建工程
    "Constructionprogress": "construction_in_process",
    # 无形资产
    "Intangibleasset": "intangible_assets",
    # 商誉
    "Goodwill": "goodwill",
    # 长期待摊费用
    "Ltdeferasset": "long_term_prepaid_expenses",
    # 递延所得税资产
    "Deferincometaxasset": "deferred_tax_assets",
    # 其他非流动资产
    "Othernonlasset": "other_non_current_assets",
    # 非流动资产合计
    "Sumnonlasset": "total_non_current_assets",
    # 资产总计
    "Sumasset": "total_assets",
    # 流动负债
    #
    # 短期借款
    "Stborrow": "short_term_borrowing",
    # 吸收存款及同业存放
    "Deposit": "accept_money_deposits",
    # 应付账款
    "Accountpay": "accounts_payable",
    # 预收款项
    "Advancereceive": "advances_from_customers",
    # 应付职工薪酬
    "Salarypay": "employee_benefits_payable",
    # 应交税费
    "Taxpay": "taxes_payable",
    # 应付利息
    "Interestpay": "interest_payable",
    # 其他应付款
    "Otherpay": "other_payable",
    # 一年内到期的非流动负债
    "Nonlliaboneyear": "current_portion_of_non_current_liabilities",
    # 其他流动负债
    "Otherlliab": "other_current_liabilities",
    # 流动负债合计
    "Sumlliab": "total_current_liabilities",
    # 非流动负债
    #
    # 长期借款
    "Ltborrow": "long_term_borrowing",
    # 长期应付款
    "Ltaccountpay": "long_term_payable",
    # 递延收益
    "Deferincome": "deferred_revenue",
    # 递延所得税负债
    "Deferincometaxliab": "deferred_tax_liabilities",
    # 其他非流动负债
    "Othernonlliab": "other_non_current_liabilities",
    # 非流动负债合计
    "Sumnonlliab": "total_non_current_liabilities",
    # 负债合计
    "Sumliab": "total_liabilities",
    # 所有者权益(或股东权益)
    #
    # 实收资本（或股本）
    "Sharecapital": "capital",
    # 资本公积
    "Capitalreserve": "capital_reserve",
    # 专项储备
    "Specialreserve": "special_reserve",
    # 盈余公积
    "Surplusreserve": "surplus_reserve",
    # 未分配利润
    "Retainedearning": "undistributed_profits",
    # 归属于母公司股东权益合计
    "Sumparentequity": "equity",
    # 少数股东权益
    "Minorityequity": "equity_as_minority_interest",
    # 股东权益合计
    "Sumshequity": "total_equity",
    # 负债和股东权益合计
    "Sumliabshequity": "total_liabilities_and_equity",

    # 银行相关
    # 资产
    # 现金及存放中央银行款项
    "Cashanddepositcbank": "fi_cash_and_deposit_in_central_bank",
    # 存放同业款项
    "Depositinfi": "fi_deposit_in_other_fi",
    # 贵金属
    "Preciousmetal": "fi_expensive_metals",
    # 拆出资金
    "Lendfund": "fi_lending_to_other_fi",
    # 以公允价值计量且其变动计入当期损益的金融资产
    "Fvaluefasset": "fi_financial_assets_effect_current_income",
    # 衍生金融资产
    "Derivefasset": "fi_financial_derivative_asset",
    # 买入返售金融资产
    "Buysellbackfasset": "fi_buying_sell_back_fi__asset",
    # 应收账款
    #
    # 应收利息
    "Interestrec": "fi_interest_receivable",
    # 发放贷款及垫款
    "Loanadvances": "fi_disbursing_loans_and_advances",
    # 可供出售金融资产
    #
    # 持有至到期投资
    "Heldmaturityinv": "fi_held_to_maturity_investment",
    # 应收款项类投资
    "Investrec": "fi_account_receivable_investment",
    # 投资性房地产
    #
    # 固定资产
    #
    # 无形资产
    #
    # 商誉
    #
    # 递延所得税资产
    #
    # 其他资产
    "Otherasset": "fi_other_asset",
    # 资产总计
    #
    # 负债
    #
    # 向中央银行借款
    "Borrowfromcbank": "fi_borrowings_from_central_bank",
    # 同业和其他金融机构存放款项
    "Fideposit": "fi_deposit_from_other_fi",
    # 拆入资金
    "Borrowfund": "fi_borrowings_from_fi",
    # 以公允价值计量且其变动计入当期损益的金融负债
    "Fvaluefliab": "fi_financial_liability_effect_current_income",
    # 衍生金融负债
    "Derivefliab": "fi_financial_derivative_liability",
    # 卖出回购金融资产款
    "Sellbuybackfasset": "fi_sell_buy_back_fi_asset",
    # 吸收存款
    "Acceptdeposit": "fi_savings_absorption",
    # 存款证及应付票据
    "Cdandbillrec": "fi_notes_payable",
    # 应付职工薪酬
    #
    # 应交税费
    #
    # 应付利息
    #
    # 预计负债
    "Anticipateliab": "fi_estimated_liabilities",
    # 应付债券
    "Bondpay": "fi_bond_payable",
    # 其他负债
    "Otherliab": "fi_other_liability",
    # 负债合计
    #
    # 所有者权益(或股东权益)
    # 股本
    "Shequity": "fi_capital",
    # 其他权益工具
    "Otherequity": "fi_other_equity_instruments",
    # 其中:优先股
    "Preferredstock": "fi_preferred_stock",
    # 资本公积
    #
    # 盈余公积
    #
    # 一般风险准备
    "Generalriskprepare": "fi_generic_risk_reserve",
    # 未分配利润
    #
    # 归属于母公司股东权益合计
    #
    # 股东权益合计
    #
    # 负债及股东权益总计

    # 券商相关
    # 资产
    #
    # 货币资金
    #
    # 其中: 客户资金存款
    "Clientfund": "fi_client_fund",
    # 结算备付金
    "Settlementprovision": "fi_deposit_reservation_for_balance",
    # 其中: 客户备付金
    "Clientprovision": "fi_client_deposit_reservation_for_balance",
    # 融出资金
    "Marginoutfund": "fi_margin_out_fund",
    # 以公允价值计量且其变动计入当期损益的金融资产
    #
    # 衍生金融资产
    #
    # 买入返售金融资产
    #
    # 应收利息
    #
    # 应收款项
    "Receivables": "fi_receivables",
    # 存出保证金
    "Gdepositpay": "fi_deposit_for_recognizance",
    # 可供出售金融资产
    #
    # 持有至到期投资
    #
    # 长期股权投资
    #
    # 固定资产
    #
    # 在建工程
    #
    # 无形资产
    #
    # 商誉
    #
    # 递延所得税资产
    #
    # 其他资产
    #
    # 资产总计
    #
    # 负债
    #
    # 短期借款
    #
    # 拆入资金
    #
    # 以公允价值计量且其变动计入当期损益的金融负债
    #
    # 衍生金融负债
    #
    # 卖出回购金融资产款
    #
    # 代理买卖证券款
    "Agenttradesecurity": "fi_receiving_as_agent",
    # 应付账款
    #
    # 应付职工薪酬
    #
    # 应交税费
    #
    # 应付利息
    #
    # 应付短期融资款
    "Shortfinancing": "fi_short_financing_payable",
    # 预计负债
    #
    # 应付债券
    #
    # 递延所得税负债
    #
    # 其他负债
    #
    # 负债合计
    #
    # 所有者权益(或股东权益)
    #
    # 股本
    #
    # 资本公积
    #
    # 其他权益工具
    #
    # 盈余公积
    #
    # 一般风险准备
    #
    # 交易风险准备
    "Traderiskprepare": "fi_trade_risk_reserve",
    # 未分配利润
    #
    # 归属于母公司股东权益合计
    #
    # 少数股东权益
    #
    # 股东权益合计
    #
    # 负债和股东权益总计

    # 保险相关
    # 应收保费
    "Premiumrec": "fi_premiums_receivable",
    "Rirec": "fi_reinsurance_premium_receivable",
    # 应收分保合同准备金
    "Ricontactreserverec": "fi_reinsurance_contract_reserve",
    # 保户质押贷款
    "Insuredpledgeloan": "fi_policy_pledge_loans",
    # 定期存款
    "Tdeposit": "fi_time_deposit",
    # 可供出售金融资产
    #
    # 持有至到期投资
    #
    # 应收款项类投资
    #
    # 应收账款
    #
    # 长期股权投资
    #
    # 存出资本保证金
    "Capitalgdepositpay": "fi_deposit_for_capital_recognizance",
    # 投资性房地产
    #
    # 固定资产
    #
    # 无形资产
    #
    # 商誉
    #
    # 递延所得税资产
    #
    # 其他资产
    #
    # 独立账户资产
    "Independentasset": "fi_capital_in_independent_accounts",
    # 资产总计
    #
    # 负债
    #
    # 短期借款
    #
    # 同业及其他金融机构存放款项
    #
    # 拆入资金
    #
    # 以公允价值计量且其变动计入当期损益的金融负债
    #
    # 衍生金融负债
    #
    # 卖出回购金融资产款
    #
    # 吸收存款
    #
    # 代理买卖证券款
    #
    # 应付账款
    #
    # 预收账款
    "Advancerec": "fi_advance_from_customers",
    # 预收保费
    "Premiumadvance": "fi_advance_premium",
    # 应付手续费及佣金
    "Commpay": "fi_fees_and_commissions_payable",
    # 应付分保账款
    "Ripay": "fi_dividend_payable_for_reinsurance",
    # 应付职工薪酬
    #
    # 应交税费
    #
    # 应付利息
    #
    # 预计负债
    #
    # 应付赔付款
    "Claimpay": "fi_claims_payable",
    # 应付保单红利
    "Policydivipay": "fi_policy_holder_dividend_payable",
    # 保户储金及投资款
    "Insureddepositinv": "fi_policy_holder_deposits_and_investment_funds",
    # 保险合同准备金
    "Contactreserve": "fi_contract_reserve",
    # 长期借款
    #
    # 应付债券
    #
    # 递延所得税负债
    #
    # 其他负债
    #
    # 独立账户负债
    "Independentliab": "fi_independent_liability",
    # 负债合计
    #
    # 所有者权益(或股东权益)
    #
    # 股本
    #
    # 资本公积
    #
    # 盈余公积
    #
    # 一般风险准备
    #
    # 未分配利润
    #
    # 归属于母公司股东权益总计
    #
    # 少数股东权益
    #
    # 股东权益合计
    #
    # 负债和股东权益总计
}


class UsStockBalanceSheetRecorder(TimestampsDataRecorder):
    region = Region.US
    provider = Provider.Yahoo
    data_schema = BalanceSheet

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, codes=None, share_para=None) -> None:
        super().__init__(entity_type=EntityType.Stock, exchanges=['nyse', 'nasdaq', 'amex', 'cme'], batch_size=batch_size,
                         force_update=force_update, sleeping_time=sleeping_time, codes=codes, share_para=share_para)

    def yh_get_balance_sheet(self, code):
        try:
            return YH.Ticker(code).balance_sheet
        except Exception as e:
            self.logger.error(f'yh_get_info, code: {code}, error: {e}')
        return None

    def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        # get stock info
        balance_sheet = self.yh_get_balance_sheet(entity.code)

        if balance_sheet is None or len(balance_sheet) == 0:
            return True, time.time() - start_point, None
        balance_sheet = balance_sheet.T

        balance_sheet['timestamp'] = balance_sheet.index

        if pd_valid(balance_sheet):
            return False, time.time() - start_point, (ref_record, self.format(entity, balance_sheet))

        return True, time.time() - start_point, None

    def format(self, entity, df):
        df.rename(colunms={}, inplace=True)

        df.rename(columns=balance_sheet_map, inplace=True)

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
