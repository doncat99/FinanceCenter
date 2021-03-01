# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_etf_stocks
from zvt.domain import StockValuation, Etf, EtfValuation
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import now_pd_timestamp


class JqChinaEtfValuationRecorder(TimeSeriesDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Etf
    data_schema = EtfValuation

    def __init__(self, entity_type=EntityType.ETF, exchanges=None, entity_ids=None,
                 codes=None, batch_size=10, force_update=False, sleeping_time=5,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, share_para=share_para)

    def record(self, entity, start, end, size, timestamps, http_session):
        if not end:
            end = now_pd_timestamp(Region.CHN)

        result_df = pd.DataFrame()
        date_range = pd.date_range(start=start, end=end, freq='1D').tolist()
        for date in date_range:
            # etf包含的个股和比例
            etf_stock_df = get_etf_stocks(region=self.region, code=entity.code, timestamp=date, provider=self.provider)

            if pd_is_not_null(etf_stock_df):
                all_pct = etf_stock_df['proportion'].sum()

                if all_pct >= 1.2 or all_pct <= 0.8:
                    self.logger.error(f'ignore etf:{entity.id}  date:{date} proportion sum:{all_pct}')
                    break

                etf_stock_df.set_index('stock_id', inplace=True)

                # 个股的估值数据
                stock_valuation_df = StockValuation.query_data(region=self.region,
                                                               entity_ids=etf_stock_df.index.to_list(),
                                                               filters=[StockValuation.timestamp == date],
                                                               index='entity_id')

                if pd_is_not_null(stock_valuation_df):
                    stock_count = len(etf_stock_df)
                    valuation_count = len(stock_valuation_df)

                    self.logger.info(
                        f'etf:{entity.id} date:{date} stock count: {stock_count},'
                        f'valuation count:{valuation_count}')

                    pct = abs(stock_count - valuation_count) / stock_count

                    if pct >= 0.2:
                        self.logger.error(f'ignore etf:{entity.id}  date:{date} pct:{pct}')
                        break

                    se = pd.Series({'timestamp': date})
                    for col in ['pe', 'pe_ttm', 'pb', 'ps', 'pcf']:
                        # PE=P/E
                        # 这里的算法为：将其价格都设为PE,那么Earning为1(亏钱为-1)，结果为 总价格(PE)/总Earning

                        value = 0
                        price = 0

                        # 权重估值
                        positive_df = stock_valuation_df[[col]][stock_valuation_df[col] > 0]
                        positive_df['count'] = 1
                        positive_df = positive_df.multiply(etf_stock_df["proportion"], axis="index")
                        if pd_is_not_null(positive_df):
                            value = positive_df['count'].sum()
                            price = positive_df[col].sum()

                        negative_df = stock_valuation_df[[col]][stock_valuation_df[col] < 0]
                        if pd_is_not_null(negative_df):
                            negative_df['count'] = 1
                            negative_df = negative_df.multiply(etf_stock_df["proportion"], axis="index")
                            value = value - negative_df['count'].sum()
                            price = price + negative_df[col].sum()

                        se[f'{col}1'] = price / value

                        # 简单算术平均估值
                        positive_df = stock_valuation_df[col][stock_valuation_df[col] > 0]
                        positive_count = len(positive_df)

                        negative_df = stock_valuation_df[col][stock_valuation_df[col] < 0]
                        negative_count = len(negative_df)

                        value = positive_count - negative_count
                        price = positive_df.sum() + abs(negative_df.sum())

                        se[col] = price / value

                    result_df = pd.concat(result_df, se.to_frame().T)

        return result_df if pd_is_not_null(result_df) else None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    # 上证50
    JqChinaEtfValuationRecorder(codes=['512290']).run()


# the __all__ is generated
__all__ = ['JqChinaEtfValuationRecorder']
