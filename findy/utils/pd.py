# -*- coding: utf-8 -*-
from typing import Union
from functools import wraps
from io import StringIO, BytesIO
import re

import pandas as pd


def validate_df(columns, instance_method=True):
    """
    Decorator that raises a ValueError if input isn't a pandas
    DataFrame or doesn't contain the proper columns. Note the DataFrame
    must be the first positional argument passed to this method.

    Arguments:
        - columns: A set of column names that the dataframe must have.
                   For example, {'open', 'high', 'low', 'close'}.
        - instance_method: Whether or not the item being decorated is
                           an instance method. Pass False to decorate
                           static methods and functions.

    Returns:
        A decorated method or function.
    """
    def method_wrapper(method):
        @wraps(method)
        def validate_wrapper(self, *args, **kwargs):
            # functions and static methods don't pass self
            # so self is the first positional argument in that case
            df = (self, *args)[0 if not instance_method else 1]
            if not isinstance(df, pd.DataFrame):
                raise ValueError('Must pass in a pandas DataFrame')
            if columns.difference(df.columns):
                raise ValueError(
                    f'Dataframe must contain the following columns: {columns}'
                )
            return method(self, *args, **kwargs)
        return validate_wrapper
    return method_wrapper


def pd_valid(df: Union[pd.DataFrame, pd.Series]):
    return df is not None and not df.empty


def index_df(df, index='timestamp', inplace=True, drop=False, time_field='timestamp'):
    if time_field:
        df[time_field] = pd.to_datetime(df[time_field])

    if inplace:
        df.set_index(index, drop=drop, inplace=inplace)
    else:
        df = df.set_index(index, drop=drop, inplace=inplace)

    if type(index) == str:
        df = df.sort_index()
    elif type(index) == list:
        df.index.names = index
        level = list(range(len(index)))
        df = df.sort_index(level=level)
    return df


def is_normal_df(df, category_field='entity_id', time_filed='timestamp'):
    if pd_valid(df):
        names = df.index.names

        if len(names) == 2 and names[0] == category_field and names[1] == time_filed:
            return True

    return False


def normal_index_df(df, category_field='entity_id', time_filed='timestamp', drop=True):
    index = [category_field, time_filed]
    if is_normal_df(df):
        return df

    return index_df(df=df, index=index, drop=drop, time_field='timestamp')


def _sanitize_label(label):
    """
    Clean up a label by removing non-letter, non-space characters and
    putting in all lowercase with underscores replacing spaces.

    Parameters:
        - label: The text you want to fix.

    Returns:
        The sanitized label.
    """
    return re.sub(r'[^\w\s]', '', label).lower().replace(' ', '_')


def label_sanitizer(method):
    """
    Decorator around a method that returns a dataframe to
    clean up all labels in said dataframe (column names and index name)
    by removing non-letter, non-space characters and
    putting in all lowercase with underscores replacing spaces.

    Parameters:
        - method: The method to wrap.

    Returns:
        A decorated method or function.
    """
    # keep the docstring of the data method for help()
    @wraps(method)
    def method_wrapper(self, *args, **kwargs):
        df = method(self, *args, **kwargs)

        # fix the column names
        df.columns = [
            _sanitize_label(col) for col in df.columns
        ]

        # fix the index name
        df.index.rename(
            _sanitize_label(df.index.name),
            inplace=True
        )
        return df
    return method_wrapper


def contain_nan(df):
    return df.isnull().sum().sum()


def fill_missing_timestamp(df):
    unique_date = df.timestamp.unique()
    stock_df_list = []

    gb = df.groupby('code', sort=False)
    for code in gb.groups:
        df_stock = gb.get_group(code)
        df_stock.set_index('timestamp', drop=True, inplace=True)
        df_stock = df_stock[~df_stock.index.duplicated(keep='first')]
        df_stock.sort_index(inplace=True)
        df_stock = df_stock.reindex(unique_date, method='ffill', copy=True)
        df_stock['code'] = code
        stock_df_list.append(df_stock)

    df = pd.concat(stock_df_list)
    df.fillna(0, inplace=True)
    df.reset_index(inplace=True)

    return df


def df_to_bytes(df):
    bytes_data = BytesIO()
    df.to_csv(bytes_data)  # write to BytesIO buffer
    bytes_data.seek(0)
    return bytes_data


def bytes_to_df(bytes_data):
    return pd.read_csv(StringIO(str(bytes_data, 'utf-8')))
