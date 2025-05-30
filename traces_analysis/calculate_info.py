import pandas as pd

path = "/home/cyrus/PycharmProjects/MLDS/simulation/traces/msr/proj_4.csv"



# 定义列名
column_names = ['Timestamp', 'Hostname', 'DiskNumber', 'Type', 'Offset', 'Size', 'ResponseTime']


df = pd.read_csv(path, header=None, names=column_names)

# 计算 Timestamp 的最大值和最小值
min_timestamp = df['Timestamp'].min()
max_timestamp = df['Timestamp'].max()

# 计算 Offset 的最大值和最小值
min_offset = df['Offset'].min()
max_offset = df['Offset'].max()

# 打印结果
print(f"Timestamp 最小值: {min_timestamp}")
print(f"Timestamp 最大值: {max_timestamp}")
print(f"Offset 最小值: {min_offset}")
print(f"Offset 最大值: {max_offset}")