# components/storage.py
import simpy
import math
from config import LBA_SIZE_BYTES, LBAS_PER_CHUNK, CHUNK_SIZE_BYTES

class StorageDevice:
    """
    模拟单个存储设备。
    论文提到每个层级可以有多个设备（特别是HDD层）。
    这里的'a'和'b'参数应与config.py中的单位一致。
    """
    def __init__(self, env, name, a_param, b_param_per_lba, is_hdd=False, num_parallel_hdd=1):
        self.env = env
        self.name = name
        self.a_param = a_param  # 固定延迟部分
        self.b_param_per_lba = b_param_per_lba  # 每LBA的可变延迟部分
        self.resource = simpy.Resource(env, capacity=1) # 每个设备是一个资源
        self.is_hdd = is_hdd
        self.num_parallel_hdd = num_parallel_hdd # 用于HDD条带化

        # 更多统计信息可以添加，如利用率、队列长度等
        self.busy_time = 0
        self.requests_served = 0

    def _calculate_service_time(self, size_bytes, operation_type='read'):
        num_lbas = math.ceil(size_bytes / LBA_SIZE_BYTES)
        service_time = self.a_param + self.b_param_per_lba * num_lbas

        if self.is_hdd:
            # 论文中HDD是8个并行读写一个chunk
            # 如果是chunk操作，并且是HDD，考虑并行带来的时间减少
            if size_bytes == LBAS_PER_CHUNK * LBA_SIZE_BYTES:
                 service_time /= self.num_parallel_hdd # 简化处理，实际更复杂
            # TODO: 论文还提到HDD可以有更复杂的包含寻道和旋转的模型

        # 论文提到SSD写操作有效慢2倍 (6.2节校准部分)
        if "SSD" in self.name and operation_type == 'write': # 简陋的判断方式
            service_time *= 2

        return service_time

    def access(self, size_bytes, operation_type='read'):
        """模拟一次设备访问，返回服务时间"""
        service_time = self._calculate_service_time(size_bytes, operation_type)
        start_time = self.env.now
        yield self.env.timeout(service_time)
        self.busy_time += service_time
        self.requests_served += 1
        # print(f"{self.env.now:.2f}: Device {self.name} finished access of {size_bytes}B, op: {operation_type}, time: {service_time:.2f}")

class StorageTier:
    """
    模拟一个存储层级，包含一个或多个StorageDevice。
    管理该层级的数据块。
    """
    def __init__(self, env, name, capacity_bytes, a_ms, b_ms_per_lba, num_devices=1, is_hdd_tier=False):
        self.env = env
        self.name = name
        self.capacity_bytes = capacity_bytes
        self.used_bytes = 0
        self.devices = [StorageDevice(env, f"{name}_dev{i}", a_ms, b_ms_per_lba,
                                      is_hdd=is_hdd_tier, num_parallel_hdd=(num_devices if is_hdd_tier else 1))
                        for i in range(num_devices)]
        # 如果层级有多个设备，需要一个机制来分配请求到具体设备，这里简化为轮询或随机
        self.next_device_idx = 0

        self.chunks = {} # 存储在该层级的数据块ID及其元数据 (e.g., dirty_flag)
                        # key: chunk_id, value: {'dirty': False, 'size_bytes': CHUNK_SIZE_BYTES}

    def _add_initial_chunk_metadata(self, chunk_id, is_dirty=False):
        """
        【新增】同步方法：用于初始时直接添加数据块元数据，不模拟IO延迟或资源竞争。
        这代表数据块“初始就存在于此”。
        """
        if chunk_id not in self.chunks:
            # 检查容量，但对于初始填充，我们通常假设容量足够
            # 或者至少要能容纳所有预设的初始数据块
            if self.used_bytes + CHUNK_SIZE_BYTES > self.capacity_bytes:
                print(f"CRITICAL WARNING: Tier {self.name} insufficient capacity during initial population for chunk {chunk_id}.")
                # 在这种情况下，可能需要重新评估配置或允许超额（不推荐）
                # 为了演示，我们假设它能被添加，但实际中应处理此错误
                pass # 或者 raise Exception("Insufficient capacity for initial population")

            self.used_bytes += CHUNK_SIZE_BYTES
            self.chunks[chunk_id] = {'dirty': is_dirty, 'size_bytes': CHUNK_SIZE_BYTES}
            # print(f"DEBUG: Tier {self.name} initially populated with chunk {chunk_id}")
        else:
            # 如果块已存在（理论上初始填充时不应发生），更新其状态
            self.chunks[chunk_id]['dirty'] = is_dirty
            # print(f"DEBUG: Tier {self.name} chunk {chunk_id} already present, updated dirty status.")
        return True

    def get_device(self):
        # 简单的轮询策略选择设备
        device = self.devices[self.next_device_idx]
        self.next_device_idx = (self.next_device_idx + 1) % len(self.devices)
        return device

    def read_chunk(self, chunk_id):
        """从该层级读取一个数据块"""
        if chunk_id not in self.chunks:
            print(f"Error: Chunk {chunk_id} not in tier {self.name} for read.")
            return None # 或者抛出异常

        device = self.get_device()
        # print(f"{self.env.now:.2f}: Tier {self.name} reading chunk {chunk_id} from {device.name}")
        with device.resource.request() as req:
            yield req
            yield self.env.process(device.access(LBAS_PER_CHUNK * LBA_SIZE_BYTES, operation_type='read'))
        # print(f"{self.env.now:.2f}: Tier {self.name} finished reading chunk {chunk_id}")
        return self.chunks[chunk_id] # 返回数据块元数据

    def write_chunk(self, chunk_id, is_dirty=True):
        """向该层级写入一个数据块"""
        if self.used_bytes + (LBAS_PER_CHUNK * LBA_SIZE_BYTES) > self.capacity_bytes and chunk_id not in self.chunks:
            print(f"Error: Tier {self.name} full, cannot write new chunk {chunk_id}.")
            return False # 或者需要有替换逻辑

        device = self.get_device()
        # print(f"{self.env.now:.2f}: Tier {self.name} writing chunk {chunk_id} to {device.name}")
        with device.resource.request() as req:
            yield req
            yield self.env.process(device.access(LBAS_PER_CHUNK * LBA_SIZE_BYTES, operation_type='write'))

        if chunk_id not in self.chunks:
            self.used_bytes += (LBAS_PER_CHUNK * LBA_SIZE_BYTES)
        self.chunks[chunk_id] = {'dirty': is_dirty, 'size_bytes': LBAS_PER_CHUNK * LBA_SIZE_BYTES}
        # print(f"{self.env.now:.2f}: Tier {self.name} finished writing chunk {chunk_id}")
        return True

    def remove_chunk(self, chunk_id):
        """从该层级移除一个数据块"""
        if chunk_id in self.chunks:
            chunk_meta = self.chunks.pop(chunk_id)
            self.used_bytes -= chunk_meta['size_bytes']
            # print(f"{self.env.now:.2f}: Tier {self.name} removed chunk {chunk_id}")
            return chunk_meta # 返回被移除数据块的元数据，如dirty状态
        return None

    def has_chunk(self, chunk_id):
        return chunk_id in self.chunks

    def get_free_space(self):
        return self.capacity_bytes - self.used_bytes

    def get_chunk_meta(self, chunk_id):
        return self.chunks.get(chunk_id)