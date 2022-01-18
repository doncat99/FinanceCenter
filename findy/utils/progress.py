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
        self.process = multiprocessing.Process(target=self.processFun, args=(sleep,))

    def __del__(self):
        if self.process.is_alive():
            self.process.join()

    def processFun(self, sleep):
        consumer = connect_kafka_consumer(progress_topic, findy_config['kafka'])
        pbars = {}

        while True:
            for msg in consumer:
                data = json.loads(msg.value)
                pbar = pbars.get(data['task'], None)
                if pbar is None:
                    position = data.get('position', None)
                    pbars[data['task']] = tqdm(total=data['total'], ncols=90, desc=data['desc'], position=position, leave=data['leave'])
                    pbar = pbars[data['task']]
                pbar.update(data['update'])
            time.sleep(sleep)

    def start(self):
        self.process.start()

    def kill(self):
        if self.process.is_alive():
            self.process.terminate()
