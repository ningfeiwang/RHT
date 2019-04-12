#!/usr/local/bin/python
# coding:utf-8

import os
import appscript
import config

for node in config.nodes_list:
    appscript.app('Terminal').do_script("ssh niw217@" + config.nodes[node]["ip"] + " 'pkill -9 python' ")

