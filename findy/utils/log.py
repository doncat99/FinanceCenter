# -*- coding: utf-8 -*-
import logging
from logging.handlers import RotatingFileHandler
import os


def init_log(log_dir, file_name='findy.log', simple_formatter=True, debug_mode=False):
    root_logger = logging.getLogger()

    # reset the handlers
    root_logger.handlers = []

    root_logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    file_name = os.path.join(log_dir, file_name)

    file_log_handler = RotatingFileHandler(file_name, maxBytes=524288000, backupCount=10)

    file_log_handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    console_log_handler = logging.StreamHandler()
    console_log_handler.setLevel(logging.CRITICAL)

    # create formatter and add it to the handlers
    if simple_formatter:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)-7s  %(process)-5s  %(processName)-15s  %(message)s")
    else:
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s  %(threadName)s  %(name)s:%(filename)s:%(lineno)s  %(funcName)s  %(message)s")

    file_log_handler.setFormatter(formatter)
    console_log_handler.setFormatter(formatter)

    # add the handlers to the logger
    root_logger.addHandler(file_log_handler)
    root_logger.addHandler(console_log_handler)

    return root_logger
