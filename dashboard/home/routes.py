# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from flask import render_template, request
from flask_login import login_required
from jinja2 import TemplateNotFound

from dashboard.home import blueprint
from dashboard.home.datahouse import data_house

NavDict = {
    'index.html':           ['Overall', 'dashboard'],
    'data-house.html':      ['DataHouse', 'dashboard'],
    'tables.html':          ['Tables', 'table_view'],
    'billing.html':         ['Billing', 'receipt_long'],
    'virtual-reality.html': ['Virtual Reality', 'view_in_ar'],
    'rtl.html':             ['RTL', 'format_textdirection_r_to_l'],
    'notifications.html':   ['Notifications', 'notifications'],
    'profile.html':         ['Profile', 'person'],
    'logout.html':          ['Logout', 'directions_run'],
}


@blueprint.route('/index')
@login_required
def index():

    return render_template('home/index.html', segment='index.html', navdict=NavDict)


@blueprint.route('/<template>')
@login_required
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        if "data-house" in template:
            return data_house(template, segment=segment, navdict=NavDict)

        # Serve the file (if exists) from app/templates/home/FILE.html
        return render_template('home/' + template, segment=segment, navdict=NavDict)

    except TemplateNotFound:
        return render_template('home/page-404.html'), 404

    except:
        return render_template('home/page-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):

    try:

        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index.html'

        return segment

    except:
        return None
