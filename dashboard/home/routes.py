# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from flask import render_template, request, current_app
from flask_login import login_required
from jinja2 import TemplateNotFound

from dashboard import db
from dashboard.home import blueprint


@blueprint.route('/index')
@login_required
def index():

    return render_template('home/index.html', segment='index')


@blueprint.route('/<template>')
@login_required
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        if "data-house" in template:
            return datahouse(template, segment)

        # Serve the file (if exists) from app/templates/home/FILE.html
        return render_template('home/' + template, segment=segment)

    except TemplateNotFound:
        return render_template('home/page-404.html'), 404

    except:
        return render_template('home/page-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):

    try:

        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index'

        return segment

    except:
        return None


def datahouse(template, segment):
    app = current_app._get_current_object()
    chn_engine = db.get_engine(app, 'chn_data')
    us_engine = db.get_engine(app, 'us_data')

    chn_stock_cnt = chn_engine.execute("select count(*) from Stock").scalar()
    us_stock_cnt = us_engine.execute("select count(*) from Stock").scalar()
    stock_cnt = {'chn_stock_cnt': chn_stock_cnt, 'us_stock_cnt': us_stock_cnt}

    return render_template(f'home/{template}', segment=segment, stock_cnt=stock_cnt)
