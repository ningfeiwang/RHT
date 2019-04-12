#!/usr/local/bin/python
# coding:utf-8

import config
from hash_ring import *

class consist_hash():
    def h_ring(self):
        node_info = config.nodes
        server_name_list = []
        for key in node_info.keys():
            server_name_list.append(key)
        servers_ring = HashRing(server_name_list)
        return servers_ring