# -*- coding: utf-8 -*-
import os
from pathlib import Path

# zvt home dir
ZVT_HOME = os.environ.get('ZVT_HOME')
if not ZVT_HOME:
    ZVT_HOME = os.path.abspath(os.path.join(Path.home(), 'zvt-home'))