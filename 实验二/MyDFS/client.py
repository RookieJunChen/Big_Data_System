import os
import socket
import time
import datetime
from io import StringIO
import math

import pandas as pd

from common import *


class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))

    def __del__(self):
        self.name_node_sock.close()

    def ls(self, dfs_path):
        # TODO: 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        # 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            cmd = "ls {}".format(dfs_path)
            self.name_node_sock.send(bytes(cmd, encoding='utf-8'))
            response_msg = self.name_node_sock.recv(BUF_SIZE)
            print(str(response_msg, encoding='utf-8'))
        except Exception as e:
            print(e)
        finally:
            pass

    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))

        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)

        global data_last
        counter = 0

        # 针对多次写入修改了循环
        for idx, row in fat.iterrows():
            if counter == 0:
                data = fp.read(int(row['blk_size']))
            else:
                data = data_last
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])

            request = "store {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data_node_sock.send(bytes(data, encoding='utf-8'))
            data_node_sock.close()
            data_last = data
            counter = (counter+1) % dfs_replication
        fp.close()

    # 按行划分传输文件
    def copyFromLocalLines(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        # 计算每个文件的行数
        line_number = int(os.popen('cat {} | wc -l'.format(local_path)).read())
        print("File size: {}".format(file_size))
        print("File lines: {:d}".format(line_number))

        # 计算每行的平均占用尺寸
        line_avg_size = int(math.ceil(file_size / line_number))

        # 每个blk里面的平均的行数
        lines_per_blk = int(math.floor(big_dfs_blk_size / line_avg_size))
        print("Lines per blk: {:d}".format(lines_per_blk))

        request = "new_fat_item {} {} {}".format(dfs_path, file_size, line_number)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个向目标DataNode发送数据块
        temp_file = local_path + '_temp'    # 用于写拆分的小文件的temp_file
        with open(local_path, "r") as origin_file:
            line_counter = 0
            for idx, row in fat.iterrows():
                # 按行写拆分的小文件，避免存在元数据在划分过程中被截断
                depart_file = open(temp_file, "w")
                for line in origin_file:
                    depart_file.write(line)
                    line_counter = line_counter + 1
                    if line_counter >= lines_per_blk:
                        depart_file.close()
                        line_counter = 0
                        break
                if depart_file:
                    depart_file.close()
                # 使用scp传输大文件
                os.system("scp {} {}:{}{}.blk{}".format(temp_file, row['host_name'], "~/MyDFS/dfs/data",
                                                        dfs_path, row['blk_no']))
        os.remove(temp_file)

    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中

        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个从目标DataNode请求数据块，而后将收到的数据块写入到本地文件中
        fp = open(local_path, "w")
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])

            request = "load {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data = data_node_sock.recv(BUF_SIZE)
            data = str(data, encoding='utf-8')
            fp.write(data)
            data_node_sock.close()
        fp.close()

    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取改文件的FAT表，获取后删除；打印FAT表；根据FAT表逐个告诉目标DataNode删除对应数据块
        # 从NameNode获取改文件的FAT表，获取后删除
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个告诉目标DataNode删除对应数据块
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])

            request = "rm {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            response_msg = data_node_sock.recv(BUF_SIZE)
            print(response_msg)
            data_node_sock.close()

    def format(self):
        request = "format"
        print(request)

        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))

        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))

            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))

            data_node_sock.close()

    def calculate(self, option, dfs_path, filename):
        # 使用datetime计算运行时间
        start = datetime.datetime.now()
        # 发送指令
        request = "calculate {} {} {}".format(option, dfs_path, filename)
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        # 接收计算出的数据
        response_msg = self.name_node_sock.recv(BUF_SIZE)
        print("The {} of the {} is {}".format(option, dfs_path+filename, str(response_msg, encoding='utf-8')))
        # 统计耗费时间
        end = datetime.datetime.now()
        last_time = end - start
        print("Takes {}.".format(last_time))




# 解析命令行参数并执行对于的命令
import sys

argv = sys.argv
argc = len(argv) - 1

client = Client()

cmd = argv[1]
if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyFromLocalByLines":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocalLines(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocalByLines <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()
elif cmd == "-calculate":
    if argc == 4:
        option = argv[2]
        dfs_path = argv[3]
        filename = argv[4]
        client.calculate(option, dfs_path, filename)
    else:
        print("Usage: python client.py -calculate <option> <dfs_path> <filename>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
