#!/usr/local/bin/python
# coding:utf-8

import os
import appscript
import config
import time

#
for node in config.nodes_list:
    cmd = "ssh niw217@" + config.nodes[node]["ip"] + " 'python ~/new/server_node.py "  + node + " 4000 10000'"
    print(cmd)
    appscript.app('Terminal').do_script(cmd)

time.sleep(10)

for node in config.nodes_list:
    cmd = "ssh niw217@" + config.nodes[node]["ip"] + " 'python ~/new/client.py 4000 10 2' "
    print(cmd)
    appscript.app('Terminal').do_script(cmd)
