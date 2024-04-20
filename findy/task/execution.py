import warnings
warnings.filterwarnings("ignore")

import logging
import os
import asyncio
import platform
import time
from datetime import datetime
import msgpack

from findy import findy_config
from findy.task import TaskArgs, TaskArgsExtend, RunMode
from findy.utils.kafka import connect_kafka_producer, publish_message
from findy.utils.progress import ProgressBarProcess, progress_topic, progress_key
from findy.utils.cache import valid, get_cache, dump_cache
import findy.vendor.aiomultiprocess as amp

logger = logging.getLogger(__name__)
kafka_producer = connect_kafka_producer(findy_config['kafka'])


async def loop_task_set(task):
    now = time.time()
    item, index, pbar_update, schedule_cache, schedule_file = task

    logger.info(f"Start Func: {item[TaskArgs.FunName.value].__name__}")
    await item[TaskArgs.FunName.value](item[TaskArgs.Extend.value])
    logger.info(f"End Func: {item[TaskArgs.FunName.value].__name__}, cost: {time.time() - now}\n")

    publish_message(kafka_producer, progress_topic, progress_key,
                    msgpack.dumps({"command": "@task-finish", "task": item[TaskArgs.Extend.value][TaskArgsExtend.TaskID.value]}))

    pbar_update['update'] = 1
    publish_message(kafka_producer, progress_topic, progress_key, msgpack.dumps(pbar_update))

    schedule_cache.update({f"{item[TaskArgs.TaskGroup.value]}_{item[TaskArgs.FunName.value].__name__}": datetime.now()})
    dump_cache(schedule_file, schedule_cache)


async def fetch_process(task_set):
    schedule_file = f'task_schedule_{task_set[0][TaskArgs.TaskGroup.value]}'
    schedule_cache = get_cache(schedule_file, default={})

    print("")
    print("parallel fetching processing...")
    print("")

    tasks_filter = [item for item in task_set if not valid(f'{item[TaskArgs.TaskGroup.value]}_{item[TaskArgs.FunName.value].__name__}', item[TaskArgs.Update.value], schedule_cache)]
    pbar_update = {"task": "main", "total": len(tasks_filter), "desc": "Total Jobs", "position": 0, "leave": True, "update": 0}
    publish_message(kafka_producer, progress_topic, progress_key, msgpack.dumps(pbar_update))
    
    tasks_args_list = [(item, index, pbar_update, schedule_cache, schedule_file) for index, item in enumerate(tasks_filter)]

    parallel_tasks_args_list = []
    for task_args in tasks_args_list:
        # add task index in desc parameter
        task_args[0][TaskArgs.Extend.value].insert(TaskArgsExtend.TaskID.value, task_args[1])

        if task_args[0][TaskArgs.Mode.value] == RunMode.Serial:
            await loop_task_set(task_args)
        else:
            parallel_tasks_args_list.append(task_args)

    task_args_len = len(parallel_tasks_args_list)
    cpus = os.cpu_count()
    multiplier = 2  # 1 if task_args_len > cpus else 2
    [task_args[0][TaskArgs.Extend.value][TaskArgsExtend.Cpus.value] * multiplier for task_args in parallel_tasks_args_list]

    # if tasks > cpus/2:
    if True:
        cpus = max(1, min(task_args_len, cpus))
        childconcurrency = max(1, round(task_args_len / cpus))

        current_os = platform.system().lower()
        if current_os != "windows":
            import uvloop
            loop_initializer = uvloop.new_event_loop
        else:
            loop_initializer = None

        async with amp.Pool(cpus, childconcurrency=childconcurrency, loop_initializer=loop_initializer) as pool:
            async for _ in pool.map(loop_task_set, parallel_tasks_args_list):
                pass
    # else:
    #     for task_args in parallel_tasks_args_list:
    #         result = await loop_task_set(task_args)


def task_execution(task_set):
    pbar = ProgressBarProcess()
    pbar.start()

    print("waiting for kafka connection.....")
    time.sleep(5)

    print("")
    print("*" * 80)
    print(f"*    Start Task: {task_set[0][TaskArgs.TaskGroup.value]}      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("*" * 80)

    asyncio.run(fetch_process(task_set))

    pbar_update = {"command": "@end"}
    publish_message(kafka_producer, progress_topic, progress_key, msgpack.dumps(pbar_update))

    pbar.join()
