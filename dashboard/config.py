# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import os
from decouple import config

from findy import findy_config


class Config(object):
    # Set up the App SECRET_KEY
    CSRF_ENABLED = True
    SECRET_KEY = config('SECRET_KEY', default='S#perS3crEt_007')

    # basedir = os.path.abspath(os.path.dirname(__file__))
    # SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'db.sqlite3')  # This will create a file in <app> FOLDER

    # PostgreSQL database
    SQLALCHEMY_FLASK_URI = '{}://{}:{}@{}:{}/{}'.format(
        config('DB_ENGINE', default='postgresql'),
        config('DB_USERNAME', default=findy_config['db_user']),
        config('DB_PASS', default=findy_config['db_pass']),
        config('DB_HOST', default=findy_config['db_host']),
        config('DB_PORT', default=findy_config['db_port']),
        config('DB_NAME', default='postgres')
    )
    SQLALCHEMY_CHN_DATA_URI = '{}://{}:{}@{}:{}/{}'.format(
        config('DB_ENGINE', default='postgresql'),
        config('DB_USERNAME', default=findy_config['db_user']),
        config('DB_PASS', default=findy_config['db_pass']),
        config('DB_HOST', default=findy_config['db_host']),
        config('DB_PORT', default=findy_config['db_port']),
        config('DB_NAME', default='findy_chn')
    )
    SQLALCHEMY_US_DATA_URI = '{}://{}:{}@{}:{}/{}'.format(
        config('DB_ENGINE', default='postgresql'),
        config('DB_USERNAME', default=findy_config['db_user']),
        config('DB_PASS', default=findy_config['db_pass']),
        config('DB_HOST', default=findy_config['db_host']),
        config('DB_PORT', default=findy_config['db_port']),
        config('DB_NAME', default='findy_us')
    )
    SQLALCHEMY_BINDS = {
        'flask': SQLALCHEMY_FLASK_URI,
        'chn_data': SQLALCHEMY_CHN_DATA_URI,
        'us_data': SQLALCHEMY_US_DATA_URI
    }
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    DEBUG = False
    DEVELOPMENT = True

    # Security
    SESSION_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_HTTPONLY = True
    REMEMBER_COOKIE_DURATION = 3600

    # # PostgreSQL database
    # SQLALCHEMY_DATABASE_URI = '{}://{}:{}@{}:{}/{}'.format(
    #     config('DB_ENGINE', default='postgresql'),
    #     config('DB_USERNAME', default='appseed'),
    #     config('DB_PASS', default='pass'),
    #     config('DB_HOST', default='localhost'),
    #     config('DB_PORT', default=5432),
    #     config('DB_NAME', default='dashboard')
    # )


class DebugConfig(Config):
    DEBUG = True


# Load all possible configurations
config_dict = {
    'Production': ProductionConfig,
    'Debug': DebugConfig
}
