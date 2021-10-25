from threading import Thread


class MyThread(Thread):
    def __init__(self, func, args):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


def read_from_txt(filename):
    ans_list = []
    with open(filename, "r") as f:  # 打开文件
        data = f.readlines()  # 读取文件
    for line in data:
        line = line.strip('\n')
        # print(line)
        try:
            ans_list.append(float(line))
        except BaseException:
            continue
    return ans_list

