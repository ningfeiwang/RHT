#!/usr/local/bin/python
# coding:utf-8

import sys

import config
import RHT
import socket
import json
import threading
from hash_ring import *
from consist_hash import *
import os

def write_log(message):
    global global_var
    _sequence = global_var
    global_var += 1
    with open('/home/niw217/new/log/server_' + threading.current_thread().name + '.log', 'a') as file:
        file.write(str(_sequence) + '\n')
        file.write(message)
        file.write('\n')


class server_nodes:
    def __init__(self, server_name, max_data_size, lock_size):
        self.lock_size = lock_size
        self.lock_map = dict()
        self.init_locks()
        self.lock_con = threading.Lock()
        self.max_data_size = max_data_size
        self.connections = []
        self.node_info = config.nodes
        self.node_list = config.nodes_list
        self.server_name = server_name
        self.rht_table = RHT.RHT(self.server_name)
        self.put_nums = 0
        self.initial()
        self.ring = consist_hash().h_ring()
        # self.init_connect()

    def init_locks(self):
        for i in range(self.lock_size):
            self.lock_map[i] = threading.Lock()

    def look_up(self, keys):
        name, ip, port = [], [], []
        length = len(self.node_list)
        for key in keys:
            for node_name in self.node_info:
                host_ip = self.node_info[node_name]["ip"]
                host_port = self.node_info[node_name]["port"]
                if node_name == self.ring.get_node(str(key)):
                    ind = self.node_list.index(node_name)
                    next_name = self.node_list[(ind + 1) % length]
                    name.append((node_name, next_name))
                    ip.append((host_ip, self.node_info[next_name]["ip"]))
                    port.append((host_port, self.node_info[next_name]["port"]))
                    break
        return name, ip, port

    def initial(self):
        self.server_map = dict()

        for node_name in self.node_info:
            self.server_map[node_name] = None
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            if node_name == self.server_name:
                self.server_map[node_name] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                self.server_map[node_name].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                # print(host_ip)
                self.server_map[node_name].bind((host_ip, int(host_port)))
                self.server_map[node_name].listen(5)
                print("server starts")
                print("ip and port: " + host_ip + ":" + host_port)

    def init_connect(self):
        for server_node in self.node_list:
            if server_node == self.server_name:
                continue
            if self.server_map[server_node] == None:
                server_host = self.node_info[server_node]["ip"]
                server_port = self.node_info[server_node]["host"]
                self.server_map[server_node] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.server_map[server_node].setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.server_map[server_node].connect((server_host, int(server_port)))
                print("connection: " + server_host + ":" + server_port)


    def operation(self, opt, key, value):
        return self.rht_table.operation(opt, key, value)

    def processing(self, conn, addr):
        while True:
            data_re = conn.recv(self.max_data_size)
            if not data_re:
                break
            by = b''
            by += data_re
            data = json.loads(by.decode("utf-8"))
            print("data", data)
            write_log("recv: " + str(data))
            flag = data["flag"]
            if flag == "1":
                mes = {}
                server_node, server_host, server_port = self.look_up(data["keys"])
                mes['server_name'] = server_node
                mes['server_host'] = server_host
                mes['server_port'] = server_port
                mes = json.dumps(mes).encode('utf-8')
                write_log("send: " + str(mes))
                conn.sendall(mes)

            elif flag == "2":
                tmp = []
                mes = {}
                mes['succ'] = "True"
                for key in data["keys"]:
                    # print(data["keys"])
                    # print(key)
                    lock_id = int(key) % self.lock_size

                    if self.lock_map[lock_id].locked() and lock_id not in tmp:
                        mes['succ'] = "False"
                        for i in tmp:
                            self.lock_map[i].release()
                        break
                    else:
                        if lock_id not in tmp:
                            tmp.append(lock_id)
                            self.lock_map[lock_id].acquire()

                if mes['succ'] == "True":
                    mes['lock_list'] = tmp
                else:
                    mes['lock_list'] = []
                mes = json.dumps(mes).encode('utf-8')
                write_log("send: " + str(mes))
                conn.sendall(mes)

            elif flag == "3":
                lock_list = data['lock_list']
                for lock in lock_list:
                    self.lock_map[lock].release()
                mes = {}
                mes['release'] = "True"
                mes = json.dumps(mes).encode('utf-8')
                write_log("send: " + str(mes))
                conn.sendall(mes)

            elif flag == "4":
                mes = {}
                if data["opt"] == "put":
                    f, val = self.operation(data["opt"], data["keys"], data["value"])
                    lock_list = data['lock_list']

                    for lock in lock_list:
                        self.lock_map[lock].release()
                else:
                    lock_id = int(data["keys"][0]) % self.lock_size
                    self.lock_map[lock_id].acquire()
                    f, val = self.operation(data["opt"], data["keys"], data["value"])
                    self.lock_map[lock_id].release()

                if f == True:
                    mes["val"] = val
                    mes["success"] = "1"
                    print("successful put: " + str(self.rht_table.put_nums))
                    print("successful get: " + str(self.rht_table.get_nums))
                else:
                    mes["val"] = None
                    mes["success"] = "0"
                mes = json.dumps(mes).encode('utf-8')
                write_log("send: " + str(mes))
                conn.sendall(mes)

    def server_start(self):
        while True:
            conn, addr = self.server_map[self.server_name].accept()
            print("connect with ", conn)
            new_thread = threading.Thread(target = self.processing, args = (conn, addr))
            new_thread.daemon = True
            new_thread.start()

if __name__ == '__main__':
    global_var = 0
    server = server_nodes(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    server.server_start()