# 训练两个模型
# model 1: proposal : features tensor -> relative order of access frequency
# model 2: control : rel order -> actions
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np



# 数据建模
# 四元组： (LBA（数据块逻辑地址), IO type, timestamp, size of req)
# 每个chunks 包含的 LBA数量: |C| = Sc / Sl , Sc 为 per chunk size, Sl为per LBA size
# n: total chunks number = nl(total LBA number) / |C|(per chunk 包含的 LBA)

# 将trace划分为  大小为 τ （tau 小T) 的 时间窗口
# 以下向量均长度为n (total chunks number)

#    [1, ... , n]
# 1. [55, 66, 77, ... , 0] 每个chunk Read的次数
# 2. [55, 66, 77, ... , 0] 每个chunk Write的次数
# 3. [110, 120, 1,... , 1] 读写请求总和的访问频率计数
# 4. [n, 8, n-1,  ... , 1] 长期时间（过去24H）每个chunk被访问的相对流行度
# 5. [1, 0, 0, 1, ... , 1] chunk是否在tier 1存在
# 6. [1, 0, 0, 1, ... , 1] chunk是否在tier 2存在
# 7. [1, 1/2, 1/3,... , 0] 相对时间 (sin)： 当前时钟时间的 sin 值 (对所有数据块相同，但在张量中为每个数据块复制一次以构成 n x 8 的形状)。
# 8. [1, 1/2, 1/3,... , 0] 相对时间 (cos): 同上

# 论文最后一句提到：
# “最后，对于每个时间步 t，状态是使用每个 I/O 请求的时间戳、chunk_no、操作 (R/W) 构建的。
# 它使用上述信息转换为形状为 n × 8 的张量。”
# 这句话可能指的是用于派生上述特征的原始输入，
# 而紧接着的“使用上述信息转换为形状为 n × 8 的张量”
# 则确认了最终的特征维度是基于前面段落描述的组件。