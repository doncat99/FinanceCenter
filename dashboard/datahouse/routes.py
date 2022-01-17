# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import os
import time
from pygtail import Pygtail
from flask import render_template, current_app, Response

from findy import findy_env
from findy import 
from dashboard import db
from dashboard.datahouse import blueprint
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
        create_tasks()

    return render_template(f'home/{template}', stock_cnt=stock_cnt, task_cnt={}, **kwargs)


def create_tasks():
    task = Tasks(taskname="", market_id="", completion="0")


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
