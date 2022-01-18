# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from datetime import datetime

from dashboard import db


class Tasks(db.Model):

    __tablename__ = 'Tasks'
    __bind_key__ = 'flask'

    id = db.Column(db.Integer, primary_key=True)
    taskname = db.Column(db.String(64), nullable=False)
    created = db.Column(db.DateTime, default=datetime.utcnow)
    owner_id = db.Column(db.Integer, db.ForeignKey('Users.id'))
    market_id = db.Column(db.String(64), nullable=False)
    completion = db.Column(db.Integer, nullable=False)

    # repr method represents how one object of this datatable
    # will look like
    def __repr__(self):
        return str(self.taskname)