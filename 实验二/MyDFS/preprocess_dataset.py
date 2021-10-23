import pandas as pd
import os

dataset_dir = "/home/dsjxtjc/2021214308/datasets/nasdaq_stock/full_history/"
dst_dir = "/home/dsjxtjc/2021214308/MyDFS/test/data.txt"
all_list = os.listdir(dataset_dir)
f = open(dst_dir, "w")
counter = 0

for item in all_list:
    local_path = dataset_dir + item
    csv_file = pd.read_csv(local_path)
    csv_file = csv_file['open']
    for i in range(csv_file.size):
        f.write(str(csv_file.iloc[i]) + "\n")
    counter = counter + 1
    print("{} already processed. {}/{}".format(item, counter, len(all_list)))

