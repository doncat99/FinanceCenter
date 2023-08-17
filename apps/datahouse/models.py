# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from datetime import datetime

from apps import db


class Tasks(db.Model):

    __tablename__ = 'Tasks'
    __bind_key__ = 'flask'

    id = db.Column(db.Integer, primary_key=True)
    created = db.Column(db.DateTime, default=datetime.utcnow)
    ownerid = db.Column(db.Integer, db.ForeignKey('Users.id'))
    marketid = db.Column(db.String(64), nullable=False)
    taskid = db.Column(db.String(64), nullable=False)
    taskicon = db.Column(db.String(64), nullable=False)
    taskname = db.Column(db.String(64), nullable=False)
    progress = db.Column(db.String(64), default='?/?')
    completion = db.Column(db.Integer, default=0)

    # repr method represents how one object of this datatable
    # will look like
    def __repr__(self):
        return str(self.taskname)
