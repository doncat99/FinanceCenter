# -*- coding: utf-8 -*-
import multiprocessing
import time
import msgpack

from tqdm.auto import tqdm

from findy import findy_config
from findy.utils.kafka import connect_kafka_producer, connect_kafka_consumer

progress_topic = 'progress_topic'
progress_key = bytes('progress_key', encoding='utf-8')


class ProgressBarProcess():
    def __init__(self, sleep=0.2):
        self.kafka_producer = None

        # 创建子进程
        self.process = multiprocessing.Process(target=self.consuming, args=(sleep,))   

    def __del__(self):
        if self.process.is_alive():
            self.process.join()
            
    def getProducer(self):
        if self.kafka_producer:
            return self.kafka_producer

        self.kafka_producer = connect_kafka_producer(findy_config['kafka'])
        return self.kafka_producer

    def consuming(self, sleep):
        consumer = connect_kafka_consumer(progress_topic, findy_config['kafka'])
        pbars = {}
        pdata = {}
        pfinish = {}

        while True:
            for msg in consumer:
                data = msgpack.loads(msg.value)

                command = data.get('command', None)
                if command == '@end':
                    return
                if command == '@task-finish':
                    task = data['task']
                    pbar = pbars.get(task, None)
                    if pbar is not None:
                        pbar.n = pdata[task]['total']
                        pbar.refresh()
                    pfinish[task] = True
                    continue

                task = data['task']
                pbar = pbars.get(task, None)
                if pbar is None:
                    position = data.get('position', None)
                    desc = data['desc'] if task == 'main' else f"    {data['desc']}"
                    pbars[task] = tqdm(total=data['total'], ncols=90, desc=desc, position=position, leave=data['leave'])
                    pdata[task] = data
                    pbar = pbars[task]

                if pfinish.get(task, None) is None:
                    pbar.update(data['update'])

            time.sleep(sleep)

    def start(self):
        self.process.start()

    def kill(self):
        if self.process.is_alive():
            self.process.terminate()

    def join(self):
        if self.process.is_alive():
            self.process.join()
