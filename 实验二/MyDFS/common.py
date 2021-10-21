dfs_replication = 3
dfs_blk_size = 4096  # * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"

data_node_port = 4308  # DataNode程序监听端口
name_node_port = 14308  # NameNode监听端口

# 集群中的主机列表
# host_list = ['localhost']  # ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05', 'thumm06']
host_list = ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05', 'thumm06']
name_node_host = "thumm01"

BUF_SIZE = dfs_blk_size * 2
