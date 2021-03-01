import pandas as pd
import numpy as np

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index
from zvt.domain.misc.holder import HkHolder
from zvt.contract.recorder import TimestampsDataRecorder
from zvt.contract.api import get_data
from zvt.recorders.joinquant.common import to_entity_id
from zvt.networking.request import jq_run_query
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY, to_pd_timestamp, now_pd_timestamp


# 这里选择继承TimestampsDataRecorder是因为
# 1)时间上就是交易日的列表,这个是可知的，可以以此为增量计算点
# 2)HkHolder数据结构的设计：
# 沪股通/深股通 每日 持有 标的(股票)的情况
# 抓取的角度是entity从Index中获取 沪股通/深股通，然后按 每日 去获取

class JoinquantHkHolderRecorder(TimestampsDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Index
    data_schema = HkHolder

    def __init__(self,
                 force_update=False, sleeping_time=5, default_size=zvt_config['batch_size'],
                 real_time=False, start_timestamp=None, end_timestamp=None) -> None:
        # 聚宽编码
        # 市场通编码    市场通名称
        # 310001    沪股通
        # 310002    深股通
        # 310003    港股通（沪）
        # 310004    港股通（深）
        codes = ['310001', '310002']

        super().__init__(EntityType.Index, ['cn'], None, codes, 10, force_update, sleeping_time,
                         default_size, real_time, 'ignore', start_timestamp, end_timestamp, 0, 0)

    def init_timestamps(self, entity, http_session):
        # 聚宽数据从2017年3月17开始
        return pd.date_range(start=to_pd_timestamp('2017-03-17'),
                             end=now_pd_timestamp(Region.CHN),
                             freq='B').tolist()

    # # 覆盖这个方式是因为，HkHolder里面entity其实是股票，而recorder中entity是 Index类型(沪股通/深股通)
    # def get_latest_saved_record(self, entity):
    #     order = eval('self.data_schema.{}.desc()'.format(self.get_evaluated_time_field()))

    #     records = get_data(region=self.region,
    #                        filters=[HkHolder.holder_code == entity.code],
    #                        provider=self.provider,
    #                        data_schema=self.data_schema,
    #                        order=order,
    #                        limit=1,
    #                        return_type='domain')
    #     if records:
    #         return records[0]
    #     return None

    def get_referenced_saved_record(self, entity):
        return get_data(region=self.region,
                        filters=[HkHolder.holder_code == entity.code],
                        provider=self.provider,
                        data_schema=self.data_schema,
                        columns=['id', self.get_evaluated_time_field()],
                        return_type='df')

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df['holder_name'] + '_' + entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(time_fmt)

    def record(self, entity, start, end, size, timestamps, http_session):
        result_df = pd.DataFrame()
        for timestamp in timestamps:
            df = jq_run_query(table='finance.STK_HK_HOLD_INFO',
                              conditions=f'link_id#=#{entity.code}&day#=#{to_time_str(timestamp)}')
            result_df = pd.concat([result_df, df])

        if pd_is_not_null(result_df):
            return result_df
        return None

    def format(self, entity, df):
        df.rename(columns={'day': 'timestamp', 'link_id': 'holder_code', 'link_name': 'holder_name'}, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['entity_id'] = df['code'].apply(lambda x: to_entity_id(entity_type=EntityType.Stock, jq_code=x))
        df['provider'] = self.provider.value
        df['code'] = df['code'].apply(lambda x: x.split('.')[0])

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    JoinquantHkHolderRecorder(sleeping_time=10).run()

# the __all__ is generated
__all__ = ['JoinquantHkHolderRecorder']
