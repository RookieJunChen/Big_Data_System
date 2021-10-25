import math
import os
import socket
import time

import numpy as np
import pandas as pd

import threading

from common import *
from tools import *


# NameNode功能
# 1. 保存文件的块存放位置信息
# 2. ls ： 获取文件/目录信息
# 3. get_fat_item： 获取文件的FAT表项
# 4. new_fat_item： 根据文件大小创建FAT表项
# 5. rm_fat_item： 删除一个FAT表项
# 6. format: 删除所有FAT表项

class NameNode:
    def run(self):  # 启动NameNode
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(5)
            print("Name node started")
            # 添加线程，用该线程检测是否有服务器挂掉
            t = threading.Thread(target=self.heart_beat)
            t.start()
            # 添加线程，用该线程检测是否有数据丢失
            t2 = threading.Thread(target=self.data_loss_check)
            t2.start()

            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("connected by {}".format(addr))

                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print("Request: {}".format(request))

                    cmd = request[0]  # 指令第一个为指令类型

                    if cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.ls(dfs_path)
                    elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.get_fat_item(dfs_path)
                    elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
                        if len(request) == 3:
                            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                            file_size = int(request[2])
                            response = self.new_fat_item(dfs_path, file_size)
                        else:
                            dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                            file_size = int(request[2])
                            line_number = int(request[3])
                            response = self.new_fat_item_lines(dfs_path, file_size, line_number)
                    elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm_fat_item(dfs_path)
                    elif cmd == "format":
                        response = self.format()
                    elif cmd == "calculate":
                        option = request[1]
                        dfs_path = request[2]
                        filename = request[3]
                        response = self.calculte(option, dfs_path, filename)
                    else:  # 其他位置指令
                        response = "Undefined command: " + " ".join(request)

                    print("Response: {}".format(response))
                    sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    break
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def ls(self, dfs_path):
        local_path = name_node_dir + dfs_path
        # 如果文件不存在，返回错误信息
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)

        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息
            with open(local_path) as f:
                response = f.read()

        return response

    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)

    def new_fat_item(self, dfs_path, file_size):
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        print(file_size, nb_blks)

        # todo 如果dfs_replication为复数时可以新增host_name的数目
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])

        counter = 0
        for i in range(nb_blks):
            if dfs_replication == 1:
                blk_no = i
                # host_name = np.random.choice(host_list, size=dfs_replication, replace=False)[0]
                # 为了负载均衡，每次轮流给每个节点分发数据
                host_name = host_list[counter]
                counter = (counter + 1) % len(host_list)
                blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
                data_pd.loc[i] = [blk_no, host_name, blk_size]
            else:  # 针对需要些多个副本的情况
                blk_no = i
                host_name_list = np.random.choice(host_list, size=dfs_replication, replace=False)
                blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
                for j in range(dfs_replication):  # 每次随机选取N个host来写入当前块（N为dfs_replication）
                    host_name = host_name_list[j]
                    data_pd.loc[(i - 1) * dfs_replication + j] = [blk_no, host_name, blk_size]

        # 获取本地路径
        local_path = name_node_dir + dfs_path

        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)

    # 按行划分传输文件
    def new_fat_item_lines(self, dfs_path, file_size, line_number):
        # 计算每一行文件占用的大小
        line_avg_size = int(math.ceil(file_size / line_number))
        # 计算每个blk中平均的行数
        lines_per_blk = int(math.floor(big_dfs_blk_size / line_avg_size))

        number_blks = int(math.ceil(line_number / lines_per_blk))

        print(file_size, line_number, line_avg_size, lines_per_blk)

        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])

        counter = 0
        for i in range(number_blks):
            blk_no = i
            # 为了负载均衡，每次轮流给每个节点分发数据
            host_name = host_list[counter]
            counter = (counter + 1) % len(host_list)
            blk_size = min(lines_per_blk * line_avg_size,
                           line_number * line_avg_size - i * lines_per_blk * line_avg_size)
            data_pd.loc[i] = [blk_no, host_name, blk_size]

        # 获取本地路径
        local_path = name_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)

    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)

    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"

    # 模拟heart_beat操作。
    def heart_beat(self):
        time.sleep(5)
        while True:  # 每隔一段时间轮询一遍所有服务器，查询是否有服务器挂掉
            for hostname in host_list:
                data_node_sock = socket.socket()
                try:
                    # 通过连接后发送 ping 命令，来轮询服务器
                    data_node_sock.connect((hostname, data_node_port))
                    data_node_sock.send(bytes("ping", encoding='utf-8'))
                except BaseException:
                    # 如果发生服务器挂掉的情况，则启动修复机制，对丢失的文件进行备份，保证data_replication不变
                    print(hostname + " disconnect!")
                    host_list.remove(hostname)
                    self.repair(hostname, '/')
                    continue
                time.sleep(0.2)
                data_node_sock.close()
                # print(hostname + ": Normal.")
            time.sleep(5)

    # 遍历NameNode目录下的所有fat表，针对每个fat表进行修复
    def repair(self, host_name, dfs_dir):
        dir = name_node_dir + dfs_dir
        all_list = os.listdir(dir)
        for item in all_list:
            path = os.path.join(dir, item)
            if os.path.isfile(path):
                self.repair_item(host_name, dfs_dir + '/' + item)
            else:
                self.repair(host_name, dfs_dir + item)

    # 对具体的某个丢失的文件（根据fat表）进行备份，保证data_replication不变
    def repair_item(self, host_name, dfs_path):
        local_path = name_node_dir + dfs_path
        fat = pd.read_csv(local_path)

        blk_dic = {}
        lost_blk = []

        # 扫描寻找丢失块，并加以记录
        for idx, row in fat.iterrows():
            block_num = row['blk_no']
            host = row['host_name']
            if str(block_num) in blk_dic.keys():
                if host != host_name:
                    blk_dic[str(block_num)].append(host)
            else:
                blk_dic[str(block_num)] = [host]
            if host == host_name:
                lost_blk.append(block_num)

        # 对每一个丢失的块进行修复
        for block in lost_blk:
            temp_host_list = host_list.copy()

            # 寻找可以使用的备份服务器
            for host in blk_dic[str(block)]:
                temp_host_list.remove(host)
            recover_host = np.random.choice(temp_host_list, size=1, replace=False)[0]
            info_host = np.random.choice(blk_dic[str(block)], size=1, replace=False)[0]

            # 从包含缺失数据块的服务器中拷贝一份放到备份服务器中
            # 拷贝数据块
            recv_data_node_sock = socket.socket()
            recv_data_node_sock.connect((info_host, data_node_port))
            blk_path = dfs_path + ".blk{}".format(block)
            request = "load {}".format(blk_path)
            recv_data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data = recv_data_node_sock.recv(BUF_SIZE)
            data = str(data, encoding='utf-8')
            recv_data_node_sock.close()

            # 存放到备份服务器中
            push_data_node_sock = socket.socket()
            push_data_node_sock.connect((recover_host, data_node_port))
            request = "store {}".format(blk_path)
            push_data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            push_data_node_sock.send(bytes(data, encoding='utf-8'))
            push_data_node_sock.close()

            print("{} was lost.".format(blk_path))
            print("Passing {} from {} to {}.".format(blk_path, info_host, recover_host))

            # 修改FAT表的内容
            fat.loc[(fat['host_name'] == host_name) & (fat['blk_no'] == block), 'host_name'] = recover_host
            fat.to_csv(local_path, index=False)

    # 检测是否有文件块丢失
    def data_loss_check(self):
        time.sleep(20)
        while True:
            self.check_dfs("/")  # 开始检查目录下的所有文件
            time.sleep(20)

    # 模拟一次检测操作，NameNode检测目录下的所有文件，查看是否存在缺失的情况
    def check_dfs(self, dfs_dir):
        dir = name_node_dir + dfs_dir
        all_list = os.listdir(dir)
        # 遍历NameNode目录下的所有fat表，检查每个fat表里对应的数据块
        for item in all_list:
            path = os.path.join(dir, item)
            if os.path.isfile(path):
                self.checkout_item(dfs_dir + '/' + item)
            else:
                self.check_dfs(dfs_dir + item)

    # 检查某个fat表下的所有数据是否正常，如出现异常则进行修复
    def checkout_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        fat = pd.read_csv(local_path)
        # 检查每一条数据块是否存在
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            request = "check {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            response_msg = str(data_node_sock.recv(BUF_SIZE), encoding='utf-8')
            data_node_sock.close()

            if response_msg == 'Normal':  # 正常情况
                continue
            else:  # 数据块发生丢失的情况，进行相关的恢复工作
                print("{} was lost.".format(blk_path))

                # 寻找包含数据块的服务器
                choice_list = fat[fat['blk_no'] == row['blk_no']]['host_name']
                for i in range(choice_list.size):
                    info_host = choice_list.iloc[i]
                    if info_host != row['host_name']:
                        break

                # 从包含缺失数据块的服务器中拷贝一份放到备份服务器中
                recv_data_node_sock = socket.socket()
                recv_data_node_sock.connect((info_host, data_node_port))
                request = "load {}".format(blk_path)
                recv_data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                data = recv_data_node_sock.recv(BUF_SIZE)
                data = str(data, encoding='utf-8')
                recv_data_node_sock.close()

                # 存放到丢失数据块服务器中
                push_data_node_sock = socket.socket()
                push_data_node_sock.connect((row['host_name'], data_node_port))
                request = "store {}".format(blk_path)
                push_data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                push_data_node_sock.send(bytes(data, encoding='utf-8'))
                push_data_node_sock.close()

                print("Passing {} from {} to {}.".format(blk_path, info_host, row['host_name']))

    # 负责计算均值或方差的命令文件，负责接收命令，调用计算，返回相关数据
    def calculte(self, option, dfs_path, filename):
        thread_list = []
        # 调用多线程，同时向多个服务器发送计算的消息
        for host in host_list:
            t = MyThread(self.calculate_comunicater, args=(option, dfs_path, filename, host))
            thread_list.append(t)
            t.start()
        map_list = []
        # 开启的多个线程收集多个服务器map出的值
        for t in thread_list:
            t.join()
            if option == "var":
                length, mean, var = t.get_result()
                map_list.append((length, mean, var))
            elif option == "mean":
                length, mean = t.get_result()
                map_list.append((length, mean))
        # 调用reducer整合数据，获得最终的整体数据
        result = self.reducer(option, map_list)
        return str(result)

    # 多线程函数，负责与各个服务器进行通信
    def calculate_comunicater(self, option, dfs_path, filename, host_name):
        # 发送计算指令
        data_node_sock = socket.socket()
        data_node_sock.connect((host_name, data_node_port))
        request = "map {} {} {}".format(option, dfs_path, filename)
        data_node_sock.send(bytes(request, encoding='utf-8'))
        # 接收服务器计算出的值
        response_msg = str(data_node_sock.recv(BUF_SIZE), encoding='utf-8')
        response = response_msg.split()
        if option == "var":
            length = int(response[0])
            mean = float(response[1])
            var = float(response[2])
            return length, mean, var
        elif option == "mean":
            length = int(response[0])
            mean = float(response[1])
            return length, mean

    # 整合数据，计算总的均值或方差
    def reducer(self, option, map_list):
        avg = 0.0
        var_t = 0.0
        total_length = 0
        # 增量计算均值
        if option == "mean":
            for length, mean in map_list:
                avg = (avg * total_length + mean * length) / (total_length + length)
                total_length = total_length + length
            return avg
        # 增量计算方差
        elif option == "var":
            for length, mean, var in map_list:
                avg_new = (avg * total_length + mean * length) / (total_length + length)
                var_t = (total_length * (var_t + (avg_new - avg) ** 2) + length * (var + (avg_new - mean) ** 2)) \
                        /  (total_length + length)
                avg = avg_new
                total_length = total_length + length
            return var_t


# 创建NameNode并启动
name_node = NameNode()
name_node.run()
