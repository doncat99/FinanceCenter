# -*- coding: utf-8 -*-
import json
import logging
import time
from typing import List, Union, Type

import pandas as pd

from findy.interface import Region, Provider, EntityType
from findy.database.schema import IntervalLevel
from findy.database.schema.datatype import Mixin, EntityMixin
from findy.database.schema.meta.stock_meta import Index, Stock
from findy.database.schema.quotes.index.index_1d_kdata import Index1dKdata
from findy.database.schema.quotes.stock.stock_1d_kdata import Stock1dKdata
from findy.database.schema.register import get_entity_schema_by_type
from findy.database.context import get_db_session
from findy.database.quote import get_entities
from findy.utils.pd import fill_missing_timestamp
from findy.utils.time import to_pd_timestamp, now_pd_timestamp


class DataListener(object):
    def on_data_loaded(self, data: pd.DataFrame) -> object:
        """

        Parameters
        ----------
        data : the data loaded at first time
        """
        raise NotImplementedError

    def on_data_changed(self, data: pd.DataFrame) -> object:
        """

        Parameters
        ----------
        data : the data added
        """
        raise NotImplementedError

    def on_entity_data_changed(self, entity: str, added_data: pd.DataFrame) -> object:
        """

        Parameters
        ----------
        entity : the entity
        added_data : the data added for the entity
        """
        pass


class DataReader(object):
    logger = logging.getLogger(__name__)

    def __init__(self,
                 region: Region,
                 data_schema: Type[Mixin],
                 entity_schema: Type[EntityMixin],
                 provider: Provider = None,
                 entity_ids: List[str] = None,
                 exchanges: List[str] = None,
                 codes: List[str] = None,
                 the_timestamp: Union[str, pd.Timestamp] = None,
                 start_timestamp: Union[str, pd.Timestamp] = None,
                 end_timestamp: Union[str, pd.Timestamp] = None,
                 columns: List = None,
                 filters: List = None,
                 order: object = None,
                 limit: int = None,
                 level: IntervalLevel = None,
                 category_field: str = 'entity_id',
                 time_field: str = 'timestamp',
                 computing_window: int = None) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.data_schema = data_schema
        self.entity_schema = entity_schema

        self.region = region
        self.provider = provider

        if end_timestamp is None:
            end_timestamp = now_pd_timestamp(self.region)

        self.the_timestamp = the_timestamp
        if the_timestamp:
            self.start_timestamp = the_timestamp
            self.end_timestamp = the_timestamp
        else:
            self.start_timestamp = start_timestamp
            self.end_timestamp = end_timestamp

        self.start_timestamp = to_pd_timestamp(self.start_timestamp)
        self.end_timestamp = to_pd_timestamp(self.end_timestamp)

        self.exchanges = exchanges

        if codes:
            if type(codes) == str:
                codes = codes.replace(' ', '')
                if codes.startswith('[') and codes.endswith(']'):
                    codes = json.loads(codes)
                else:
                    codes = codes.split(',')

        self.codes = codes
        self.entity_ids = entity_ids
        self.filters = filters
        self.order = order
        self.limit = limit

        if level:
            self.level = IntervalLevel(level)
        else:
            self.level = level

        self.category_field = category_field
        self.time_field = time_field
        self.computing_window = computing_window

        self.category_col = eval(f'self.data_schema.{self.category_field}')
        self.time_col = eval(f'self.data_schema.{self.time_field}')

        self.columns = columns

        # we store the data in a multiple index(category_column,timestamp) Dataframe
        if self.columns:
            # support str
            if type(columns[0]) == str:
                self.columns = []
                for col in columns:
                    self.columns.append(eval(f'data_schema.{col}'))

            # always add category_column and time_field for normalizing
            self.columns = list(set(self.columns) | {self.category_col, self.time_col})

        self.data_listeners: List[DataListener] = []

        self.data_df: pd.DataFrame = None

        # self.load_data()

    async def load_window_df(self, data_schema, window):
        window_df = None

        db_session = get_db_session(self.region, self.provider, data_schema)

        dfs = []
        for entity_id in self.entity_ids:
            data, column_names = data_schema.query_data(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                index=[self.category_field, self.time_field],
                order=data_schema.timestamp.desc(),
                entity_id=entity_id,
                limit=window)

            if data and len(data) > 0:
                df = pd.DataFrame([s.__dict__ for s in data], columns=column_names)
                dfs.append(df)
        if dfs:
            window_df = pd.concat(dfs)
            window_df = window_df.sort_index(level=[0, 1])
        return window_df

    def load_data(self):
        self.logger.info('load_data start')
        start_time = time.time()

        db_session = get_db_session(self.region, self.provider, self.data_schema)

        # params = dict(entity_ids=self.entity_ids, provider=self.provider,
        #               columns=self.columns, start_timestamp=self.start_timestamp,
        #               end_timestamp=self.end_timestamp, filters=self.filters,
        #               order=self.order, limit=self.limit, level=self.level,
        #               index=[self.category_field, self.time_field],
        #               time_field=self.time_field)
        # self.logger.info(f'query_data params:{params}')

        # 转换成标准entity_id
        if self.entity_schema and not self.entity_ids:
            entities, column_names = get_entities(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_schema=self.entity_schema,
                exchanges=self.exchanges,
                codes=self.codes,
                columns=[self.entity_schema.entity_id])

            if len(entities) > 0:
                self.entity_ids = [entity.entity_id for entity in entities]
            else:
                return

        data, column_names = self.data_schema.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_ids=self.entity_ids,
            columns=self.columns,
            start_timestamp=self.start_timestamp,
            end_timestamp=self.end_timestamp,
            filters=self.filters,
            order=self.order,
            limit=self.limit,
            level=self.level,
            index=[self.category_field, self.time_field],
            time_field=self.time_field)

        if data and not self.columns:
            self.data_df = pd.DataFrame([s.__dict__ for s in data], columns=column_names)
        else:
            self.data_df = pd.DataFrame(data, columns=column_names)

        cost_time = time.time() - start_time
        self.logger.info(f'load_data finished, cost_time:{cost_time}')

        for listener in self.data_listeners:
            listener.on_data_loaded(self.data_df)


def load_ticker(tickers, start, end, return_type='mix'):
    ticker_type = {'index': [Index1dKdata, Index],
                   'stock': [Stock1dKdata, Stock]}
    result_dict = {}

    for key, value in ticker_type.items():
        reader = DataReader(region=Region.US,
                            provider=Provider.Yahoo,
                            codes=tickers,
                            data_schema=value[0],
                            start_timestamp=start,
                            end_timestamp=end,
                            entity_schema=value[1])

        reader.load_data()
        if reader.data_df is None:
            continue

        # convert the column names to standardized names
        reader.data_df = reader.data_df[[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "code",
        ]]

        reader.data_df.reset_index(drop=True, inplace=True)
        result_dict[key] = reader.data_df.copy()

    data_df_list = []
    ticker_type_code = {}

    for key, value in result_dict.items():
        ticker_type_code[key] = value.code.unique().tolist()
        data_df_list.append(value)

    data_df = fill_missing_timestamp(pd.concat(data_df_list))

    # create day of the week column (monday = 0)
    # data_df["day"] = data_df["timestamp"].dt.dayofweek

    if return_type == 'mix':
        data_df = data_df.sort_values(by=['timestamp', 'code']).reset_index(drop=True)
        return data_df

    if return_type == 'separate_type':
        data_df = data_df.sort_values(by=['timestamp', 'code']).reset_index(drop=True)
        for type_key, stock_list in ticker_type_code.items():
            result_dict[type_key] = data_df.loc[data_df['code'].isin(stock_list)]
        return result_dict

    if return_type == 'separate_stock':
        for type_key, stock_list in ticker_type_code.items():
            type_data_dict = {}
            for stock in stock_list:
                type_data_dict[stock] = data_df[data_df['code'] == stock]
            result_dict[type_key] = type_data_dict
        return result_dict

    return None


def load_company_info(tickers=None):
    entity_schema = get_entity_schema_by_type(EntityType.StockDetail)
    db_session = get_db_session(Region.US, Provider.Yahoo, entity_schema)
    entities, column_names = get_entities(
        region=Region.US,
        provider=Provider.Yahoo,
        entity_schema=entity_schema,
        db_session=db_session,
        codes=tickers)

    df = pd.DataFrame([s.__dict__ for s in entities], columns=column_names)
    df.reset_index(drop=True, inplace=True)

    return df
