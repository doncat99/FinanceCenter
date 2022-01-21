# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import os
import json
import random
import time
from pygtail import Pygtail
from flask import render_template, current_app, Response, flash

from findy import findy_env, findy_config
from findy.interface import Region
from findy.interface.fetch import Para, task_set_chn, task_set_us
from findy.utils.kafka import connect_kafka_consumer
from findy.utils.progress import progress_topic

from dashboard import db
from dashboard.home import blueprint
from dashboard.datahouse.models import Tasks

LOG_FILE = os.path.join(findy_env['log_path'], 'findy.log')


def data_house(template, **kwargs):
    app = current_app._get_current_object()
    chn_engine = db.get_engine(app, 'chn_data')
    us_engine = db.get_engine(app, 'us_data')

    chn_stock_cnt = chn_engine.execute("select count(*) from Stock").scalar()
    us_stock_cnt = us_engine.execute("select count(*) from Stock").scalar()

    chn_etf_cnt = chn_engine.execute("select count(*) from Etf_Stock").scalar()
    us_etf_cnt = us_engine.execute("select count(*) from Etf_Stock").scalar()

    stock_cnt = {'chn_stock_cnt': chn_stock_cnt, 'us_stock_cnt': us_stock_cnt,
                 'chn_etf_cnt': chn_etf_cnt, 'us_etf_cnt': us_etf_cnt}

    tasks = Tasks.query.all()
    if len(tasks) == 0:
        tasks = create_tasks()

    return render_template(f'home/{template}', stock_cnt=stock_cnt, task_cnt=tasks, **kwargs)


def create_tasks():
    tasks = []

    mypath = os.path.join(os.getcwd(), 'dashboard', 'static', 'assets', 'img', 'small-logos')
    onlyfiles = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]

    try:
        for item in task_set_chn:
            task = Tasks(taskid=item[Para.TaskID.value],
                         taskicon=random.choice(onlyfiles),
                         taskname=item[Para.Desc.value],
                         marketid=Region.CHN.value,
                         completion="0")
            db.session.add(task)
            tasks.append(task)

        for item in task_set_us:
            task = Tasks(taskid=item[Para.TaskID.value],
                         taskicon=random.choice(onlyfiles),
                         taskname=item[Para.Desc.value],
                         marketid=Region.US.value,
                         completion="0")
            db.session.add(task)
            tasks.append(task)

        db.session.commit()

    except Exception as e:
        print(e)
        flash("Could Not Add Task!", category="error")

    return tasks


@blueprint.route('/log')
def progress_log():
    def generate():
        while True:
            file = Pygtail(LOG_FILE, every_n=1)
            for index, line in enumerate(file):
                yield "data:" + str(line) + "\n\n"
                time.sleep(0.1)
            time.sleep(1)
    return Response(generate(), mimetype='text/event-stream')


@blueprint.route('/progress')
def progress():
    pbars = {}
    pdata = {}
    pfinish = {}
    ptasks = {}
    sleep = 0.2
    tasks = Tasks.query.all()
    for task in tasks:
        ptasks[task.taskid] = task

    def generate():
        consumer = connect_kafka_consumer(progress_topic, findy_config['kafka'])
        while True:
            for msg in consumer:
                data = json.loads(msg.value)

                command = data.get('command', None)
                if command == '@end':
                    return
                if command == '@task-finish':
                    task = data['task']
                    pbar = pbars.get(task, None)
                    if pbar is not None:
                        pbar['update'] = pdata[task]['total']
                    pfinish[task] = True
                else:
                    task = data['task']
                    pbar = pbars.get(task, None)
                    if pbar is None:
                        pbars[task] = {'update': data['update'], 'total': data['total'], 'completion': 0}
                        pdata[task] = data
                        pbar = pbars[task]

                    if pfinish.get(task, None) is None:
                        pbar['update'] += data['update']
                    pbar['progress'] = f"{pbar['update']}/{pbar['total']}"
                    pbar['completion'] = int(pbar['update'] / pbar['total'] * 100)

                    ptasks[task]['progress'] = pbar['progress']
                    ptasks[task]['completion'] = pbar['completion']

                db.session.commit()
                ret_string = "data:" + json.dumps(pbars) + "\n\n"
                print(ret_string)
                yield ret_string
            time.sleep(sleep)

    return Response(generate(), mimetype='text/event-stream')
