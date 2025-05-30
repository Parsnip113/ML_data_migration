# main.py
import simpy
import statistics
import csv
from config import SIMULATION_TIME, TIER_CONFIGS, TRACE_FILE_PATH, TOTAL_CHUNKS, CHUNK_SIZE_MB, LBAS_PER_CHUNK, CHUNK_SIZE_BYTES, LBA_SIZE_BYTES
from components.storage import StorageTier
from components.orchestrator import Orchestrator
from components.request_generator import RequestGenerator
from components.migration_controller import MigrationController
from components.policy import SimpleLFUPolicy # 或后续的AITPolicy

def run_simulation():
    print("Starting MLDS Simulation Environment...")
    env = simpy.Environment()

    # 1. 初始化存储层级
    tiers = []
    for i, tc in enumerate(TIER_CONFIGS):
        is_hdd = "HDD" in tc['name']
        tier = StorageTier(env, tc['name'],
                           tc['capacity_MB'] * 1024 * 1024,
                           tc['a_ms'], tc['b_ms_per_lba'],
                           num_devices=tc['num_devices'],
                           is_hdd_tier=is_hdd)
        tiers.append(tier)
        print(f"Initialized {tier.name} with capacity {tc['capacity_MB']} MB")

    # 2. 初始化协调器
    orchestrator = Orchestrator(env, tiers) # rg_ref 稍后设置

    # 3. 初始化请求生成器
    # 确保trace文件存在且格式正确
    request_generator = RequestGenerator(env, orchestrator, TRACE_FILE_PATH, TOTAL_CHUNKS)
    orchestrator.set_request_generator(request_generator) # 设置回调引用

    # 4. 初始化策略模块 (这里用简单的LFU示例)
    # 后续这里会替换为 AITPolicy，它内部会加载和使用PyTorch模型
    policy_config = {} # 可以传递一些特定于策略的配置
    # active_policy = SimpleLFUPolicy(env, orchestrator, tiers, policy_config)
    # TODO: 在这里实例化您的 AITPolicy 类
    # from components.ait_policy import AITPolicy # 假设您创建了这个文件
    # ait_model_path = "path/to/your/trained_ait_model.pth"
    # active_policy = AITPolicy(env, orchestrator, tiers, policy_config, model_path=ait_model_path, n_chunks=TOTAL_CHUNKS)
    active_policy = SimpleLFUPolicy(env, orchestrator, tiers, policy_config)


    # 5. 初始化迁移控制器
    migration_controller = MigrationController(env, orchestrator, active_policy, request_generator)

    # 运行模拟
    print(f"\nRunning simulation for {SIMULATION_TIME} environment time units...")
    env.run(until=SIMULATION_TIME * 1.2) # 运行给一点buffer确保所有事件处理完

    print("\nSimulation finished.")
    print("-------------------- STATISTICS --------------------")
    if request_generator.latencies:
        avg_latency = statistics.mean(request_generator.latencies)
        p95_latency = statistics.quantiles(request_generator.latencies, n=100)[94] # p95
        print(f"Total Requests Generated: {request_generator.requests_generated}")
        print(f"Total Requests Completed: {request_generator.completed_requests}")
        print(f"Average I/O Latency: {avg_latency:.2f} ms")
        print(f"P95 I/O Latency: {p95_latency:.2f} ms")
    else:
        print("No requests completed to calculate latency.")

    for i, tier in enumerate(tiers):
        print(f"\n--- {tier.name} ---")
        print(f"  Used Space: {tier.used_bytes / (1024*1024):.2f} MB / {tier.capacity_bytes / (1024*1024):.2f} MB")
        print(f"  Number of Chunks: {len(tier.chunks)}")
        for j, device in enumerate(tier.devices):
            print(f"  Device {j}:")
            print(f"    Requests Served: {device.requests_served}")
            print(f"    Total Busy Time: {device.busy_time:.2f} ms")
            if env.now > 0 :
                utilization = (device.busy_time / env.now) * 100
                print(f"    Utilization: {utilization:.2f}%")
    print("--------------------------------------------------")


if __name__ == "__main__":
    # 您需要创建一个简单的CSV trace文件，例如 'traces/sample_trace.csv'
    # timestamp,lba,size_bytes,type
    # 0.0,1000,4096,read
    # 0.01,20000,8192,write
    # 0.05,1024,4096,read
    # ...
    # 确保 traces 文件夹存在
    import os
    if not os.path.exists(TRACE_FILE_PATH):
      raise FileNotFoundError(f"错误：追踪文件 '{TRACE_FILE_PATH}' 不存在。程序已终止。")


    run_simulation()