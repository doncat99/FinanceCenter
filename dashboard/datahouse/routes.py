# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import os
import random
import time
from pygtail import Pygtail
from flask import render_template, current_app, Response, flash

from findy import findy_env
from findy.interface import Region
from findy.interface.fetch import Para, task_set_chn, task_set_us

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
        for task in task_set_chn:
            task = Tasks(taskicon=random.choice(onlyfiles), taskname=task[Para.Desc.value], market_id=Region.CHN.value, completion="0")
            db.session.add(task)
            tasks.append(task)

        for task in task_set_us:
            task = Tasks(taskicon=random.choice(onlyfiles), taskname=task[Para.Desc.value], market_id=Region.US.value, completion="0")
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
