#!/usr/local/bin/python
# coding:utf-8
import config
import socket
import json
import random
import sys
import time
from collections import deque
import threading
from threading import Thread, Lock
import numpy as np
import select

def write_log(message):
    global global_var
    _sequence = global_var
    global_var += 1
    with open('/home/niw217/new/log/coordinator_' + threading.current_thread().name + '.log', 'a') as file:
        file.write(str(_sequence) + '\n')
        file.write(message)
        file.write('\n')

class client:
    def __init__(self, max_data_size):
        self.lock_ = threading.Lock()
        self.max_data_size = max_data_size
        self.node_info = config.nodes
        self.node_list = config.nodes_list
        self.server_map = dict()
        self.initial()
        self.put_nums = 0
        self.put_suc = 0
        self.get_nums = 0
        self.get_suc = 0
        self.fail_put = deque([])
        self.server_map_fail = dict()
        self.thread_fail = Thread(target=self.handle_fail)
        self.thread_fail.daemon = True
        self.thread_fail.start()


    def initial(self):
        for node_name in self.node_info:
            print(node_name)
            self.server_map[node_name] = None
            # self.server_map_fail[node_name] = None
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_socket_fail = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_map[node_name] = send_socket
            # self.server_map_fail[node_name] = send_socket_fail
            send_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            send_socket.connect((host_ip, int(host_port)))
            # send_socket_fail.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # send_socket_fail.connect((host_ip, int(host_port)))

            print('connected with :', host_ip, host_port)

    def handle_fail(self):
        while True:
            if len(self.fail_put) > 0:
                # print('len', str(len(self.fail_put)))
                self.lock_.acquire()
                keys, values = self.fail_put.popleft()
                self.operation("put", keys, values, 1)
                self.lock_.release()
                time.sleep(0.00001)


    def operation(self, opt, keys, value = [None], fflag = 0):
        # self.lock_.acquire()

        if fflag == 0:
            self.lock_.acquire()

        mes = dict()
        lock_flag = 1
        lock_list = {}
        mes["opt"] = opt
        mes["keys"] = keys
        mes["value"] = value
        # mes_encode = json.dumps(mes).encode('utf-8')
        ran_server = random.randint(0, len(self.node_list) - 1)
        target = self.node_list[ran_server]
        mes["flag"] = "1"
        # print(mes)
        mes_encode = json.dumps(mes).encode('utf-8')
        self.server_map[target].sendall(mes_encode)
        print("message send to ", target)
        res = self.server_map[target].recv(self.max_data_size)


        by = b''
        by += res
        # print('by', by)
        data = json.loads(by.decode("utf-8"))

        server_node = data['server_name']
        server_host = data['server_host']
        server_port = data['server_port']


        if opt == "get":
            mes = {}
            mes["flag"] = "4"
            mes["keys"] = keys
            mes["value"] = value
            mes["opt"] = opt
            mes_encode = json.dumps(mes).encode('utf-8')
            self.server_map[server_node[0][0]].sendall(mes_encode)
            res = self.server_map[server_node[0][0]].recv(self.max_data_size)


            # for i in range(2):
            #     self.server_map[server_node[0][i]].sendall(mes_encode)
            # self.server_map[server_node[0][0]].settimeout(0.1)
            # self.server_map[server_node[0][1]].settimeout(0.1)
            # while True:
            #     try:
            #         res = self.server_map[server_node[0][0]].recv(self.max_data_size)
            #         break
            #     except:
            #         try:
            #             res = self.server_map[server_node[0][1]].recv(self.max_data_size)
            #             break
            #         except:
            #             continue
            #

            # self.server_map[server_node[0][0]].settimeout(None)
            # self.server_map[server_node[0][1]].settimeout(None)
            by = b''
            by += res
            data_fin = json.loads(by.decode("utf-8"))
            # exit()
            if opt == "put":
                self.put_nums += 1
                if data_fin["success"] == "1":
                    self.put_suc += 1

            if opt == "get":
                self.get_nums += 1
                if data_fin["success"] == "1":
                    self.get_suc += 1

        else:
            server_ = dict()
            print(server_node)

            for ind, content in enumerate(server_node):
                for se in content:
                    if se not in server_:
                        server_[se] = [ind]
                    else:
                        server_[se].append(ind)
            print("server_", server_)
            server_can = []
            for server in server_:
                mes_state2 = {}
                mes_state2["flag"] = "2"
                mes_state2["opt"] = opt
                mes_state2["keys"] = [keys[i] for i in server_[server]]
                # if len(mes_state2["keys"]) == 0:
                #     continue
                server_can.append(server)
                # print('mes_state2["keys"]',mes_state2["keys"])
                mes_state2["value"] = [value[i] for i in server_[server]]
                mes_encode = json.dumps(mes_state2).encode('utf-8')

                # Done: write log in coordinator
                write_log("prepare_message: " + str(mes_encode))

                self.server_map[server].sendall(mes_encode)

                print("message send to ", server)
                res = self.server_map[server].recv(self.max_data_size)
                by = b''
                by += res
                data = json.loads(by.decode("utf-8"))
                if data["succ"] == "False":
                    lock_flag = 0

                lock_list[server] = data["lock_list"]
            # state 3
            if lock_flag == 0:
                for server in server_can:
                    mes_state3 = {}
                    mes_state3["flag"] = "3"
                    mes_state3["lock_list"] = lock_list[server]
                    mes_encode = json.dumps(mes_state3).encode('utf-8')

                    # Done: write log in coordinator
                    write_log("abort: " + str(mes_encode))

                    self.server_map[server].sendall(mes_encode)
                    print("message send to ", server)
                    res = self.server_map[server].recv(self.max_data_size)
                    by = b''
                    by += res
                    res = json.loads(by.decode("utf-8"))
                    # res = json.dumps(res).encode('utf-8')
                    print("release result: ", res["release"])
                    self.fail_put.append((keys, value))

            # state 4 two threads
            else:
                for server in server_:
                    mes_state4 = {}
                    mes_state4["flag"] = "4"
                    mes_state4["lock_list"] = lock_list[server]
                    mes_state4["opt"] = opt
                    mes_state4["keys"] = keys
                    mes_state4["value"] = value
                    mes_encode = json.dumps(mes_state4).encode('utf-8')

                    # Done: write log in coordinator
                    write_log("commit: " + str(mes_encode))

                    self.server_map[server].sendall(mes_encode)
                    print("message send to ", server)
                    res = self.server_map[server].recv(self.max_data_size)
                    by = b''
                    by += res
                    data_fin = json.loads(by.decode("utf-8"))


                if opt == "put":
                    self.put_nums += 1
                    if data_fin["success"] == "1":
                        self.put_suc += 1

                if opt == "get":
                    self.get_nums += 1
                    if data_fin["success"] == "1":
                        self.get_suc += 1
        if fflag == 0:
            self.lock_.release()


    def close(self):
        for key in self.server_map.keys():
            self.server_map[key].close()

    def summary(self):
        print("the total number of successful put operations: ", int(self.put_suc))
        print("the total number of non-successful put operations: ", int(self.put_nums - self.put_suc))
        print("the total number of get operations that returned a value different from NULL: ", int(self.get_suc))
        print("the total number of get operations that returned a NULL value: ", int(self.get_nums - self.get_suc))

def main():

    range_keys = int(sys.argv[2])
    client_ = client(int(sys.argv[1]))
    time_list = []
    for i in range(int(sys.argv[3])):
        # print(i)
        rand = random.uniform(0, 1)
        key = random.randint(0, range_keys)

        if rand <= 0.6:
            opt = "get"
            client_.operation(opt, [key])
        elif rand <= 0.8:
            opt = "put"
            value = random.uniform(0, 10000)
            client_.operation(opt, [key], [value])
        else:
            opt = "put"
            keys = np.random.randint(range_keys, size=3)
            keys = keys.tolist()

            values = np.random.uniform(0,10000,size=3)
            values = values.tolist()
            client_.operation(opt, keys, values)

    while True:
        if len(client_.fail_put) == 0:
            client_.summary()
            client_.close()

            break

    # print(float(sys.argv[3])/(end - start))



if __name__ == '__main__':
    global_var = 0
    thread_list = []
    for i in range(8):
        thread_list.append(Thread(target=main))
        thread_list[i].daemon = False
    for i in range(8):
        thread_list[i].start()