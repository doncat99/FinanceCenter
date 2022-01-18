# -*- coding: utf-8 -*-
import multiprocessing
import time
import json

from tqdm.auto import tqdm

from findy import findy_config
from findy.utils.kafka import connect_kafka_consumer

progress_topic = 'progress_topic'
progress_key = 'progress_key'


class ProgressBarProcess():
    def __init__(self, sleep=0.2):
        # 创建子进程
        self.process = multiprocessing.Process(target=self.processFun)
        self.sleep = sleep
        self.pbar = {}

    def __del__(self):
        if self.process.is_alive():
            self.process.join()

    def create_bar(self, key, bar_len, ncols=90, desc="Parallel Jobs", position=0, leave=False):
        self.pbar[key] = tqdm(total=bar_len, ncols=ncols, desc=desc, leave=leave)
        return self.pbar[key]

    def get_bar(self, data):
        data = json.loads(data.value)
        pbar = self.pbar.get(data['task'], None)
        if pbar is not None:
            return pbar

        return self.create_bar(data['task'], data['total'], desc=data['desc'], leave=data['leave'])

    def processFun(self):
        consumer = connect_kafka_consumer(progress_topic, findy_config['kafka'])
        while True:
            for msg in consumer:
                self.get_bar(msg).update()
            time.sleep(self.sleep)

    def start(self):
        self.process.start()

    def kill(self):
        if self.process.is_alive():
            self.process.terminate()
