# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.database.schema.fundamental.finance import IncomeStatement
from findy.database.plugins.eastmoney.common import to_report_period_type
from findy.database.plugins.eastmoney.finance.base_china_stock_finance_recorder import BaseChinaStockFinanceRecorder
from findy.utils.convert import to_float

income_statement_map = {
    # 营业总收入
    #
    # 营业收入
    "Operatereve": "operating_income",
    # 营业总成本
    "Totaloperateexp": "total_operating_costs",
    # 营业成本
    "Operateexp": "operating_costs",
    # 研发费用
    "Rdexp": "rd_costs",
    # 提取保险合同准备金净额
    "Netcontactreserve": "net_change_in_insurance_contract_reserves",
    # 营业税金及附加
    "Operatetax": "business_taxes_and_surcharges",
    # 销售费用
    "Saleexp": "sales_costs",
    # 管理费用
    "Manageexp": "managing_costs",
    # 财务费用
    "Financeexp": "financing_costs",
    # 资产减值损失
    "Assetdevalueloss": "assets_devaluation",
    # 其他经营收益
    #
    # 加: 投资收益
    "Investincome": "investment_income",
    # 其中: 对联营企业和合营企业的投资收益
    "Investjointincome": "investment_income_from_related_enterprise",
    # 营业利润
    "Operateprofit": "operating_profit",
    # 加: 营业外收入
    "Nonoperatereve": "non_operating_income",
    # 减: 营业外支出
    "Nonoperateexp": "non_operating_costs",
    # 其中: 非流动资产处置净损失
    "Nonlassetnetloss": "loss_on_disposal_non_current_asset",

    # 利润总额
    "Sumprofit": "total_profits",
    # 减: 所得税费用
    "Incometax": "tax_expense",
    # 净利润
    "Netprofit": "net_profit",
    # 其中: 归属于母公司股东的净利润
    "Parentnetprofit": "net_profit_as_parent",
    # 少数股东损益
    "Minorityincome": "net_profit_as_minority_interest",
    # 扣除非经常性损益后的净利润
    "Kcfjcxsyjlr": "deducted_net_profit",
    # 每股收益
    # 基本每股收益
    "Basiceps": "eps",
    # 稀释每股收益
    "Dilutedeps": "diluted_eps",
    # 其他综合收益
    "Othercincome": "other_comprehensive_income",
    # 归属于母公司股东的其他综合收益
    "Parentothercincome": "other_comprehensive_income_as_parent",
    # 归属于少数股东的其他综合收益
    "Minorityothercincome": "other_comprehensive_income_as_minority_interest",
    # 综合收益总额
    "Sumcincome": "total_comprehensive_income",
    # 归属于母公司所有者的综合收益总额
    "Parentcincome": "total_comprehensive_income_as_parent",
    # 归属于少数股东的综合收益总额
    "Minoritycincome": "total_comprehensive_income_as_minority_interest",

    # 银行相关
    # 利息净收入
    "Intnreve": "fi_net_interest_income",
    # 其中:利息收入
    "Intreve": "fi_interest_income",
    # 利息支出
    "Intexp": "fi_interest_expenses",
    # 手续费及佣金净收入
    "Commnreve": "fi_net_incomes_from_fees_and_commissions",
    # 其中:手续费及佣金收入
    "Commreve": "fi_incomes_from_fees_and_commissions",
    # 手续费及佣金支出
    "Commexp": "fi_expenses_for_fees_and_commissions",
    # 公允价值变动收益
    "Fvalueincome": "fi_income_from_fair_value_change",
    # 汇兑收益
    "Exchangeincome": "fi_income_from_exchange",
    # 其他业务收入
    "Otherreve": "fi_other_income",
    # 业务及管理费
    "Operatemanageexp": "fi_operate_and_manage_expenses",

    # 保险相关
    # 已赚保费
    "Premiumearned": "fi_net_income_from_premium",
    # 其中:保险业务收入
    "Insurreve": "fi_income_from_premium",
    # 分保费收入
    "Rireve": "fi_income_from_reinsurance_premium",
    # 减:分出保费
    "Ripremium": "fi_reinsurance_premium",
    # 提取未到期责任准备金
    "Unduereserve": "fi_undue_duty_reserve",
    # 银行业务利息净收入
    "Bankintnreve": "fi_net_income_from_bank_interest",
    # 其中:银行业务利息收入
    "Bankintreve": "fi_income_from_bank_interest",
    # 银行业务利息支出
    "Bankintexp": "fi_expenses_for_bank_interest",
    # 非保险业务手续费及佣金净收入
    "Ninsurcommnreve": "fi_net_incomes_from_fees_and_commissions_of_non_insurance",
    # 非保险业务手续费及佣金收入
    "Ninsurcommreve": "fi_incomes_from_fees_and_commissions_of_non_insurance",
    # 非保险业务手续费及佣金支出
    "Ninsurcommexp": "fi_expenses_for_fees_and_commissions_of_non_insurance",
    # 退保金
    "Surrenderpremium": "fi_insurance_surrender_costs",
    # 赔付支出
    "Indemnityexp": "fi_insurance_claims_expenses",
    # 减:摊回赔付支出
    "Amortiseindemnityexp": "fi_amortized_insurance_claims_expenses",
    # 提取保险责任准备金
    "Dutyreserve": "fi_insurance_duty_reserve",
    # 减:摊回保险责任准备金
    "Amortisedutyreserve": "fi_amortized_insurance_duty_reserve",
    # 保单红利支出
    "Policydiviexp": "fi_dividend_expenses_to_insured",
    # 分保费用
    "Riexp": "fi_reinsurance_expenses",
    # 减:摊回分保费用
    "Amortiseriexp": "fi_amortized_reinsurance_expenses",
    # 其他业务成本
    "Otherexp": "fi_other_op_expenses",

    # 券商相关
    # 手续费及佣金净收入
    #
    # 其中:代理买卖证券业务净收入
    "Agenttradesecurity": "fi_net_incomes_from_trading_agent",
    # 证券承销业务净收入
    "Securityuw": "fi_net_incomes_from_underwriting",
    # 受托客户资产管理业务净收入
    "Clientassetmanage": "fi_net_incomes_from_customer_asset_management",
    # 手续费及佣金净收入其他项目
    "Commnreveother": "fi_fees_from_other",
    # 公允价值变动收益
    #
    # 其中:可供出售金融资产公允价值变动损益
    "Fvalueosalable": "fi_income_from_fair_value_change_of_fi_salable",
}


class ChinaStockIncomeStatementRecorder(BaseChinaStockFinanceRecorder):
    data_schema = IncomeStatement

    url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetLiRunBiaoList'
    finance_report_type = 'LiRunBiaoList'

    data_type = 2

    def format(self, entity, df):
        cols = list(df.columns)
        str_cols = ['Title']
        date_cols = [self.get_original_time_field()]
        float_cols = list(set(cols) - set(str_cols) - set(date_cols))
        for column in float_cols:
            df[column] = df[column].apply(lambda x: to_float(x[0]))

        df.rename(columns=income_statement_map, inplace=True)

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
