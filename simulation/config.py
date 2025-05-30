# config.py


# 1 min = 60000 ms
# 模拟相关
SIMULATION_TIME = 60000 * 60 * 12 # 模拟总时长 模拟环境单位: ms
WINDOW_SIZE = 60000 * 10     # 决策窗口大小 (论文中提到，例如5分钟)

# 数据块/LBA大小 (论文中提到LBA大小512B，数据块大小8MB, systor使用128MB)
LBA_SIZE_BYTES = 512
CHUNK_SIZE_MB = 8
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
LBAS_PER_CHUNK = CHUNK_SIZE_BYTES // LBA_SIZE_BYTES


# --- Trace Configuration ---
# 可选值: "MSR", "GENERIC_CSV", "CBS", "SYSTOR17"
TRACE_FORMAT = "MSR" # <<--- 修改这里来选择不同的解析器

LOGS_DIR = "/home/cyrus/PycharmProjects/MLDS/simulation/logs"
OUTPUT_DIR = "/home/cyrus/PycharmProjects/MLDS/simulation/simulation_output"
TRACE_FORMAT_OPTIONS = {
    "GENERIC_CSV": {"has_header": True}, # 示例
    "CBS": {"has_header": False} # 示例
}
# 存储层级配置 (示例，需要根据论文和实际情况调整)
# (name, capacity_bytes, device_type, 'a' (base_latency_ms), 'b' (per_lba_latency_ms), num_devices)
# 论文中时间单位是ms还是us需要注意，这里统一用ms示例
# 假设 'a' 和 'b' 是从论文6.2节校准的，但要注意单位统一！
TIER_CONFIGS_SYS17 = [
    {'name': 'Tier1_Optane', 'capacity_MB': 1024 * 32,  'a_ms': 0.0002, 'b_ms_per_lba': 0.00026, 'num_devices': 1}, # Optane: a=0.2us, b=0.26us/LBA
    {'name': 'Tier2_SSD',    'capacity_MB': 1024 * 512, 'a_ms': 0.06,   'b_ms_per_lba': 0.0005,  'num_devices': 1}, # SSD: a=60us, b=0.5us/LBA
    {'name': 'Tier3_HDD',    'capacity_MB': 524288 * 10, 'a_ms': 4.0,    'b_ms_per_lba': 0.002,   'num_devices': 8}  # HDD: a=4ms, b=2us/LBA. 论文中HDD是8个并行
]
TIER_CONFIGS_MSR = [
    {'name': 'Tier1_Optane', 'capacity_MB': 512,  'a_ms': 0.0002, 'b_ms_per_lba': 0.00026, 'num_devices': 1}, # Optane: a=0.2us, b=0.26us/LBA
    {'name': 'Tier2_SSD',    'capacity_MB': 1024 * 4, 'a_ms': 0.06,   'b_ms_per_lba': 0.0005,  'num_devices': 1}, # SSD: a=60us, b=0.5us/LBA
    {'name': 'Tier3_HDD',    'capacity_MB': 1024 * 24, 'a_ms': 4.0,    'b_ms_per_lba': 0.002,   'num_devices': 8}  # HDD: a=4ms, b=2us/LBA. 论文中HDD是8个并行
    # 24 G
]
TIER_CONFIGS = [
    {'name': 'Tier1_Optane', 'capacity_MB': 1024 * 16,  'a_ms': 0.0002, 'b_ms_per_lba': 0.00026, 'num_devices': 1}, # Optane: a=0.2us, b=0.26us/LBA
    {'name': 'Tier2_SSD',    'capacity_MB': 1024 * 64, 'a_ms': 0.06,   'b_ms_per_lba': 0.0005,  'num_devices': 1}, # SSD: a=60us, b=0.5us/LBA
    {'name': 'Tier3_HDD',    'capacity_MB': 1024 * 256, 'a_ms': 4.0,    'b_ms_per_lba': 0.002,   'num_devices': 8}  # HDD: a=4ms, b=2us/LBA. 论文中HDD是8个并行
]
# sys17 32G 512G * 10
# LBA总数 (需要根据你的trace或系统设定)


TOTAL_LBAS_MSR= 1024 * 1024 * 512
TOTAL_LBAS = 1024 * 1024 * 512
TOTAL_LBAS_SYS17 = 1024 * 1024 * 1024 * 10 # 假设一个比较大的LBA空间，能容纳所有数据块  // MSR 中最大offset 17437548544 + 4096 Bytes
TOTAL_CHUNKS = TOTAL_LBAS // LBAS_PER_CHUNK

# 追踪文件路径
TRACE_FILE_PATH = "/home/cyrus/PycharmProjects/MLDS/simulation/traces/msr/proj_4.csv" # 您需要准备一个追踪文件



# trace文件特征


# 1. MSR
# ...




# 2. sys17
# (Timestamp,Response,IOType,LUN,Offset,Size)
#
# The timestamp is given as a Unix time (seconds since 1/1/1970) with a fractional part.
# Although the fractional part is nine digits, it is accurate only to the microsecond level;
# please  ignore the nanosecond part.
# If you need to process the timestamps in their original local timezone, it is UTC+0900 (JST).
#

#
# 3. k5cloud
# ...
# 4. tencent_cloud



# 模拟环境
# Req
# (req_id, ts_orig, lba, size, type, arri_time_simu)
# ReqGen.access_log
# (env.time, chunk_id, type, size_bytes)


