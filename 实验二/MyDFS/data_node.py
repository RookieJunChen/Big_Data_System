import os
import socket
import re
import numpy as np

from common import *
from tools import *


# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

class DataNode:
    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)

                    cmd = request[0]  # 指令第一个为指令类型

                    if cmd == "load":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.load(dfs_path)
                    elif cmd == "store":  # 存储数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.store(sock_fd, dfs_path)
                    elif cmd == "check":  # 检查数据块是否存在
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.check(dfs_path)
                    elif cmd == "rm":  # 删除数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm(dfs_path)
                    elif cmd == "format":  # 格式化DFS
                        response = self.format()
                    elif cmd == "map":
                        option = request[1]
                        dfs_path = request[2]
                        filename = request[3]
                        response = self.mapper(dfs_path, filename, option)
                    else:
                        response = "Undefined command: " + " ".join(request)

                    sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:
                    break
                finally:
                    sock_fd.close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            listen_fd.close()

    # check指令，用于检测本地的数据块是否丢失
    def check(self, dfs_path):
        # 本地路径
        local_path = data_node_dir + dfs_path
        if os.path.exists(local_path):
            return "Normal"
        else:
            return "Disappear"

    def load(self, dfs_path):
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)

        return chunk_data

    def store(self, sock_fd, dfs_path):
        # 从Client获取块数据
        chunk_data = sock_fd.recv(BUF_SIZE)
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 将数据块写入本地文件
        with open(local_path, "wb") as f:
            f.write(chunk_data)

        return "Store chunk {} successfully~".format(local_path)

    def rm(self, dfs_path):
        local_path = data_node_dir + dfs_path
        rm_command = "rm -rf " + local_path
        os.system(rm_command)

        return "Remove chunk {} successfully~".format(local_path)

    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)

        return "Format datanode successfully~"

    # 在data_node上执行mapper操作
    def mapper(self, dfs_path, filename, option):
        # 使用正则表达式寻找符合条件的分块文件
        read_format = re.compile(r"{}.(\w)*".format(filename))
        total_list = []

        # 对本服务器下所有的该文件的分块文件进行统计，计算均值或方差
        all_list = os.listdir(data_node_dir + dfs_path)
        for file in all_list:
            if not read_format.search(file) == None:
                read_file = read_format.search(file).group(0)
                read_dir = data_node_dir + dfs_path + read_file
                data_list = read_from_txt(read_dir)
                total_list.extend(data_list)

        # 均值无论是在求均值还是方差时都会用到，故必须计算
        # 如果操作是求方差，则额外求方差
        if option == "var":
            mean = np.mean(total_list)
            var = np.var(total_list)
            length = len(total_list)
            return "{} {} {}".format(length, mean, var)
        elif option == "mean":
            mean = np.mean(total_list)
            length = len(total_list)
            return "{} {}".format(length, mean)


# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
