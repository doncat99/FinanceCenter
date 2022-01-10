# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""
import time
from pygtail import Pygtail
from flask import render_template, current_app, Response


from dashboard import db
from dashboard.home import blueprint


def data_house(template, segment):
    app = current_app._get_current_object()
    chn_engine = db.get_engine(app, 'chn_data')
    us_engine = db.get_engine(app, 'us_data')

    chn_stock_cnt = chn_engine.execute("select count(*) from Stock").scalar()
    us_stock_cnt = us_engine.execute("select count(*) from Stock").scalar()

    chn_etf_cnt = chn_engine.execute("select count(*) from Etf_Stock").scalar()
    us_etf_cnt = us_engine.execute("select count(*) from Etf_Stock").scalar()

    stock_cnt = {'chn_stock_cnt': chn_stock_cnt, 'us_stock_cnt': us_stock_cnt,
                 'chn_etf_cnt': chn_etf_cnt, 'us_etf_cnt': us_etf_cnt}

    return render_template(f'home/{template}', segment=segment, stock_cnt=stock_cnt)


@blueprint.route('/progress')
def progress():
    def generate():
        x = 0
        while x <= 100:
            yield str(x) + "\n\n"
            x = x + 10
            time.sleep(0.1)
    return Response(generate(), mimetype='text/event-stream')


@blueprint.route('/log')
def progress_log():
    LOG_FILE = '/Users/huangdon/findy-home/logs/findy.log'

    def generate():
        while True:
            for line in Pygtail(LOG_FILE, every_n=1):
                yield str(line)
                time.sleep(0.1)
            time.sleep(1)
    return Response(generate(), mimetype='text/event-stream')
