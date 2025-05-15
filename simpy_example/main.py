import simpy
import random

# --- 参数设置 ---
RANDOM_SEED = 42          # 随机数种子，用于结果可复现
NUM_MACHINES = 1          # 洗车机数量
WASHTIME = 5              # 每辆车的洗车时间 (分钟)
T_INTERARRIVAL_MEAN = 7   # 车辆平均到达间隔时间 (分钟)
SIM_TIME = 60             # 总模拟时长 (分钟)

# 用于收集等待时间的列表
wait_times = []

class Carwash(object):
    """
    一个洗车店对象，拥有一定数量的洗车机。
    """
    def __init__(self, env, num_machines):
        self.env = env
        self.machine = simpy.Resource(env, capacity=num_machines) # 定义洗车机为资源

    def wash(self, car_name):
        """
        单个车辆的洗车过程。
        """
        print(f'{self.env.now:.2f}: {car_name} 开始洗车')
        yield self.env.timeout(WASHTIME) # 模拟洗车时间
        print(f'{self.env.now:.2f}: {car_name} 洗车完毕')


def car(env, name, cw_machine_resource, carwash_instance):
    """
    代表一辆车的过程。它到达，请求洗车机，被清洗，然后离开。
    env: SimPy 环境
    name: 车辆的名字 (用于打印日志)
    cw_machine_resource: 代表洗车机的 simpy.Resource 对象
    carwash_instance: Carwash 类的实例 (虽然在这个简单例子中 wash 方法可以在 car 内部实现，
                      但用 Carwash 类封装资源和操作是良好实践)
    """
    arrival_time = env.now
    print(f'{env.now:.2f}: {name} 到达洗车店')

    # 请求洗车机资源
    # 'with' 语句会自动处理资源的请求和释放
    with cw_machine_resource.request() as request:
        yield request  # 等待直到获得洗车机资源

        # 获得了洗车机
        waiting_time = env.now - arrival_time
        wait_times.append(waiting_time) # 记录等待时间
        print(f'{env.now:.2f}: {name} 进入洗车机 (等待了 {waiting_time:.2f} 分钟)')

        # 调用洗车过程 (属于 Carwash 实例的方法)
        yield env.process(carwash_instance.wash(name))
        # 或者，如果洗车逻辑简单，可以直接在这里 timeout:
        # yield env.timeout(WASHTIME)
        # print(f'{env.now:.2f}: {name} 洗车完毕 (在 {env.now - (arrival_time + waiting_time):.2f} 分钟内)')

    print(f'{env.now:.2f}: {name} 离开洗车店')


def setup(env, num_machines, washtime_per_car, mean_interarrival_time):
    """
    创建并启动模拟。
    env: SimPy 环境
    num_machines: 洗车机数量
    washtime_per_car: 每辆车的洗车时间
    mean_interarrival_time: 车辆平均到达间隔
    """
    # 创建 Carwash 实例
    carwash_obj = Carwash(env, num_machines)

    # 创建初始车辆 (如果需要在模拟开始时就有车辆)
    # env.process(car(env, 'Car 0', carwash_obj.machine, carwash_obj))

    # 创建车辆生成器 (源)
    car_id = 0
    while True:
        # 模拟车辆到达的间隔时间 (指数分布)
        yield env.timeout(random.expovariate(1.0 / mean_interarrival_time))
        car_id += 1
        env.process(car(env, f'Car {car_id}', carwash_obj.machine, carwash_obj))


# --- 启动模拟 ---
print('--- 洗车店模拟开始 ---')
random.seed(RANDOM_SEED) # 设置随机种子

# 创建 SimPy 环境
env = simpy.Environment()

# 启动 setup 过程，它会负责创建 Carwash 实例和车辆生成器
env.process(setup(env, NUM_MACHINES, WASHTIME, T_INTERARRIVAL_MEAN))

# 运行模拟直到指定时间
env.run(until=SIM_TIME)

print('--- 洗车店模拟结束 ---')

# --- 结果分析 ---
if wait_times:
    average_wait_time = sum(wait_times) / len(wait_times)
    print(f'\n平均等待时间: {average_wait_time:.2f} 分钟')
    print(f'总共服务车辆数: {len(wait_times)}')
else:
    print('\n模拟时间内没有车辆完成洗车。')