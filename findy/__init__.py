# -*- coding: utf-8 -*-
import os
import sys
import json
import pkg_resources
from pathlib import Path

import pandas as pd

from findy.utils.log import init_log

sys.setrecursionlimit(1000000)

pd.options.compute.use_bottleneck = True
pd.options.compute.use_numba = False
pd.options.compute.use_numexpr = True
pd.set_option('expand_frame_repr', False)
pd.set_option('mode.chained_assignment', 'raise')
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)

findy_env = {}

findy_config = {}

FINDY_HOME = os.environ.get('FINDY_HOME')
if not FINDY_HOME:
    FINDY_HOME = os.path.abspath(os.path.join(Path.home(), 'findy-home'))


def init_config(pkg_name: str = None, current_config: dict = None, **kwargs) -> dict:
    """
    init config
    """

    # create default config.json if not exist
    if pkg_name:
        config_file = f'{pkg_name}_config.json'
    else:
        pkg_name = 'findy'
        config_file = 'config.json'

    config_path = os.path.join(findy_env['findy_home'], config_file)
    if not os.path.exists(config_path):
        from shutil import copyfile
        try:
            sample_config = pkg_resources.resource_filename(pkg_name, 'config.json')
            if os.path.exists(sample_config):
                copyfile(sample_config, config_path)
        except Exception as e:
            print(f'could not load config.json from package {pkg_name}')
            raise e

    if os.path.exists(config_path):
        with open(config_path) as f:
            config_json = json.load(f)
            for k in config_json:
                current_config[k] = config_json[k]

    # set and save the config
    for k in kwargs:
        current_config[k] = kwargs[k]
        with open(config_path, 'w+') as outfile:
            json.dump(current_config, outfile)

    return current_config


def init_env(findy_home: str, **kwargs) -> dict:

    findy_env['findy_home'] = findy_home

    # path for storing cache
    findy_env['log_path'] = os.path.join(findy_home, 'logs')
    os.makedirs(findy_env['log_path'], exist_ok=True)

    # path for storing cache
    findy_env['cache_path'] = os.path.join(findy_home, 'cache')
    os.makedirs(findy_env['cache_path'], exist_ok=True)

    # path for storing cache
    findy_env['out_path'] = os.path.join(findy_home, 'out')
    os.makedirs(findy_env['out_path'], exist_ok=True)

    # path for 3th-party source
    findy_env['source_path'] = os.path.join(findy_home, 'source')
    os.makedirs(findy_env['source_path'], exist_ok=True)

    # init config
    init_config(current_config=findy_config, **kwargs)

    init_log(findy_env['log_path'], debug_mode=findy_config['debug'])

    return findy_env


init_env(findy_home=FINDY_HOME)


import findy.database.plugins as findy_plugins


__all__ = ['findy_plugins']
