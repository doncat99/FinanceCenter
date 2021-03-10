# -*- coding: utf-8 -*-
import logging
# import time
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
# pool = None


# def get_pool():
#     global pool
#     if pool is None:
#         pool = multiprocessing.Pool(8)
#     return pool


def create_mp_share_value():
    return multiprocessing.Value('i', 0)


def progress_count(total_count, desc, prog_count):
    pbar = tqdm(total=total_count, desc=desc, ncols=90, position=1, leave=True)
    while True:
        if pbar.n >= total_count:
            break
        update_cnt = prog_count.value - pbar.n
        if update_cnt > 0:
            pbar.update(update_cnt)


# def run_func(self, task_queue, func, prog_count):
#     http_session = get_http_session(self.mode)

#     while True:
#         # logger.info(f'{self.name} running, task empty: {self.task_queue.empty()}')
#         if task_queue.empty():
#             # logger.info(f'{self.name} break loop')
#             return
#         entity = task_queue.get()
#         task_queue.task_done()

#         try:
#             func(entity, http_session)
#         except Exception as e:
#             logger.error(f'{self.func.__name__} entity:{entity} error {e}')
#         # symbol = entity if isinstance(entity, str) else entity.code
#         # self.result_queue.put(f'{symbol} done')
#         prog_count.value += 1


# def run_amp(mode, cpu_cnt, func, entities, desc, prog_count):
#     # # Task queue is used to send the entities to processes
#     # # Result queue is used to get the result from processes
#     tq = multiprocessing.JoinableQueue()   # task queue
#     # rq = multiprocessing.Queue()           # result queue

#     entities = entities[:100]
#     entity_cnt = len(entities)

#     # spawning multiprocessing limited by the available cores
#     if zvt_config['debug']:
#         cpus = 1
#     else:
#         if cpu_cnt != 0:
#             cpus = min(zvt_config['processes'], multiprocessing.cpu_count() * 2, entity_cnt, cpu_cnt)
#         else:
#             cpus = min(zvt_config['processes'], multiprocessing.cpu_count() * 2, entity_cnt)

#     random.shuffle(entities)
#     [tq.put(entity) for entity in entities]

#     pbar = multiprocessing.Process(name='pbar', target=progress_count, args=(entity_cnt, desc, prog_count))
#     pbar.daemon = True
#     pbar.start()

#     pool = get_pool()

#     for _ in range(cpus):
#         pool.apply_async(run_func, (tq, func, prog_count, ))

#     tq.join()
#     logger.info('task queue joined')

#     pbar.terminate()


def run_amp(mode, cpu_cnt, func, entities, desc, prog_count):
    # # Task queue is used to send the entities to processes
    # # Result queue is used to get the result from processes
    tq = multiprocessing.JoinableQueue()   # task queue
    # rq = multiprocessing.Queue()           # result queue

    entity_cnt = len(entities)

    # spawning multiprocessing limited by the available cores
    if zvt_config['debug']:
        cpus = 1
    else:
        if cpu_cnt != 0:
            cpus = min(zvt_config['processes'], multiprocessing.cpu_count() * 2, entity_cnt, cpu_cnt)
        else:
            cpus = min(zvt_config['processes'], multiprocessing.cpu_count() * 2, entity_cnt)

    pbar = multiprocessing.Process(name='pbar', target=progress_count, args=(entity_cnt, desc, prog_count))
    pbar.daemon = True
    pbar.start()

    random.shuffle(entities)
    [tq.put(entity) for entity in entities]

    multiprocesses = [AMP(tq, func, mode, prog_count) for i in range(cpus)]
    [process.start() for process in multiprocesses]

    tq.join()
    logger.info('task queue joined')

    [process.join() for process in multiprocesses]

    pbar.terminate()


class AMP(multiprocessing.Process):
    ''' Ticker class fetches stocker ticker daily data. '''
    def __init__(self, task_queue, func, mode, prog_count):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        # self.result_queue = result_queue
        self.func = func
        self.mode = mode
        self.prog_count = prog_count
        # self.daemon = True

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
            entities_allocated.append(entity)
            self.task_queue.task_done()

        logger.info(f'processing {self.mode} {len(entities_allocated)} entities')
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.async_process(entities_allocated, self.prog_count))

    def run_sync(self):
        http_session = get_http_session(self.mode)

        # Get all tasks
        while True:
            # logger.info(f'{self.name} running, task empty: {self.task_queue.empty()}')
            if self.task_queue.empty():
                # logger.info(f'{self.name} break loop')
                return
            entity = self.task_queue.get()
            self.task_queue.task_done()
            try:
                self.func(entity, http_session)
            except Exception as e:
                logger.error(f'{self.func.__name__} entity:{entity} error {e}')
            # symbol = entity if isinstance(entity, str) else entity.code
            # self.result_queue.put(f'{symbol} done')
            self.prog_count.value += 1

    def run(self):
        self.mode = RunMode.Sync

        # Do sync or async processing
        if self.mode == RunMode.Async:
            self.run_async()
        else:
            self.run_sync()
