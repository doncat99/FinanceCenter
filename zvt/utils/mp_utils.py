# -*- coding: utf-8 -*-
import logging
import time
import random
import functools
import multiprocessing

import asyncio
import uvloop
# import aiofiles
from tqdm.auto import tqdm

from zvt import zvt_config
from zvt.api.data_type import RunMode
from zvt.networking.request import get_http_session

logger = logging.getLogger(__name__)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def create_mp_share_value():
    return multiprocessing.Value('i', 0)


def progress_count(total_count, desc, prog_count):
    pbar = tqdm(total=total_count, desc=desc, ncols=80, leave=True)
    while True:
        update_cnt = prog_count.value - pbar.n
        if update_cnt > 0:
            pbar.update(update_cnt)
        if pbar.n >= total_count:
            break


def run_amp(mode, process_cnt, func, entities, desc, prog_count):
    entity_cnt = len(entities)

    progress_bar = multiprocessing.Process(
        name='ProgressBar', target=progress_count, args=(entity_cnt, desc, prog_count))
    progress_bar.start()

    # spawning multiprocessing limited by the available cores
    if zvt_config['debug']:
        processes = 1
    else:
        if process_cnt != 0:
            # processes = min(zvt_config['processes'], multiprocessing.cpu_count(), entity_cnt, process_cnt)
            processes = min(zvt_config['processes'], entity_cnt, process_cnt)
        else:
            # processes = min(zvt_config['processes'], multiprocessing.cpu_count(), entity_cnt)
            processes = min(zvt_config['processes'], entity_cnt)

    # # Task queue is used to send the entities to processes
    # # Result queue is used to get the result from processes
    tq = multiprocessing.JoinableQueue()   # task queue
    rq = multiprocessing.Queue()           # result queue

    multiprocesses = [AMP(tq, rq, func, mode, prog_count) for i in range(processes)]
    for process in multiprocesses:
        logger.info(f'{process.name} process start')
        process.start()

    random.shuffle(entities)
    [tq.put(entity) for entity in entities]
    [tq.put(None) for _ in range(processes)]

    tq.join()
    logger.info('task queue joined, processes finished')

    time.sleep(1)

    logger.info('join processes start')
    for process in multiprocesses:
        process.terminate()
        time.sleep(0.1)
        if not process.is_alive():
            logger.info(f'{process.name} process join start')
            process.join(timeout=1.0)
            logger.info(f'{process.name} process join done')

    prog_count.value = entity_cnt

    progress_bar.terminate()
    logger.info('progressbar terminated')
    time.sleep(0.1)
    progress_bar.join(timeout=1.0)
    logger.info('progressbar joined')


class AMP(multiprocessing.Process):
    ''' Ticker class fetches stocker ticker daily data. '''
    def __init__(self, task_queue, result_queue, func, mode, prog_count):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.func = func
        self.mode = mode
        self.prog_count = prog_count

    # async def aioprocess(self, ticker: str, http_session: ClientSession) -> str:
    #     """Issue GET for the ticker and write to file."""
    #     logger.info(f'{self.name} processing_ticker {ticker}')
    #     fname = f'{self.odir}/{ticker}.csv'
    #     res = await self.get(ticker=ticker, http_session=http_session)
    #     if not res:
    #         return f'{ticker} fetch failed'
    #     async with aiofiles.open(fname, "a") as f:
    #         await f.write(res)
    #     return f'{ticker} fetch succeeded'

    @staticmethod
    def async_callback(entity, result_queue, prog_count, future):
        symbol = entity if isinstance(entity, str) else entity.code
        result_queue.put(f'{symbol} done')
        prog_count.value += 1
        # time.sleep(0.01)

    async def async_process(self, entities, prog_count):
        """Create http_session to concurrently fetch tickers."""
        logger.info(f'{self.name} http_session for {len(entities)} entities')
        http_session = get_http_session(self.mode)

        tasks = []
        for entity in entities:
            task = asyncio.create_task(self.func(entity, http_session))
            task.add_done_callback(functools.partial(self.async_callback, entity, self.result_queue, prog_count))
            tasks.append(task)

        await asyncio.wait(tasks)

        await http_session.close()

    def run_async(self):
        entities_allocated = []

        # Get all tasks
        while True:
            entity = self.task_queue.get()
            if entity is None:
                logger.info(f'{self.name} Received all allocated entities')
                break
            entities_allocated.append(entity)
            self.task_queue.task_done()

        logger.info(f'processing {self.mode} {len(entities_allocated)} entities')

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_process(entities_allocated, self.prog_count))

        # Respond to None received in task_queue
        self.task_queue.task_done()

    def run_sync(self):
        http_session = get_http_session(self.mode)

        # Get all tasks
        while True:
            entity = self.task_queue.get()
            if entity is None:
                logger.info(f'{self.name} Received all allocated entities')
                break
            try:
                self.func(entity, http_session)
            except Exception as e:
                logger.error(f'{self.func.__name__} entity:{entity} error {e}')
            symbol = entity if isinstance(entity, str) else entity.code
            self.result_queue.put(f'{symbol} done')
            self.prog_count.value += 1
            self.task_queue.task_done()

        # Respond to None received in task_queue
        self.task_queue.task_done()

    def run(self):
        self.mode = RunMode.Sync

        # Do sync or async processing
        if self.mode == RunMode.Async:
            self.run_async()
        else:
            self.run_sync()
