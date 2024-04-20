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
    Extend = 5


class TaskArgsExtend(enum.Enum):
    Region = 0
    Provider = 1
    Sleep = 2
    TaskID = 3
    Cpus = 4
    Concurrent = 5
    Desc = 6
    keyword = 7