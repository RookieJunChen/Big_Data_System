from tools import *
import datetime
import numpy as np

# 计算均值并统计时间
start = datetime.datetime.now()
long_list = read_from_txt("/home/dsjxtjc/2021214308/MyDFS/test/data.txt")
print("mean:{}".format(np.mean(long_list)))
end = datetime.datetime.now()
last_time = end - start
print("Takes {}.".format(last_time))

# 计算方差并统计时间
start = datetime.datetime.now()
long_list = read_from_txt("/home/dsjxtjc/2021214308/MyDFS/test/data.txt")
print("var: {}".format(np.var(long_list)))
end = datetime.datetime.now()
last_time = end - start
print("Takes {}.".format(last_time))
