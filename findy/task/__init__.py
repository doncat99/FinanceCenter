# -*- coding: utf-8 -*-
import enum


class RunMode(enum.Enum):
    Serial = 0
    Parallel = 1


class TaskArgs(enum.Enum):
    TaskGroup = 0
    TaskID = 1
    Mode = 2
    Update = 3
    FunName = 4
    Argument = 5
