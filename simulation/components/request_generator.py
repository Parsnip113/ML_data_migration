# components/request_generator.py
import simpy
# import csv # 不再直接使用csv，除非解析器内部需要
from config import LBA_SIZE_BYTES, LBAS_PER_CHUNK, CHUNK_SIZE_BYTES, SIMULATION_TIME
from config import TRACE_FORMAT, TRACE_FORMAT_OPTIONS # 新增导入
from components.trace_parser import get_parser, RawTraceEntry # 新增导入

class Request: # (这个类可以保持和之前MSR版本类似)
    def __init__(self, req_id, timestamp_orig_raw, # 可以存储最原始的时间戳字符串/数字
                 lba, size_bytes, req_type, arrival_time_in_sim,
                 hostname=None, disk_num=None, orig_response_time_raw=None,
                 extra_fields=None):
        self.id = req_id
        self.timestamp_orig_raw = timestamp_orig_raw
        self.lba = lba
        self.size_bytes = size_bytes
        self.req_type = req_type # 'read' or 'write'
        self.arrival_time_in_sim = arrival_time_in_sim

        self.hostname = hostname
        self.disk_num = disk_num
        self.orig_response_time_raw = orig_response_time_raw
        self.extra_fields = extra_fields if extra_fields else {}

        self.completion_time_in_sim = -1
        self.latency = -1

    def get_chunk_id_and_offset(self):
        chunk_id = self.lba // LBAS_PER_CHUNK
        offset_in_chunk_lbas = self.lba % LBAS_PER_CHUNK
        return chunk_id, offset_in_chunk_lbas

class RequestGenerator:
    def __init__(self, env, orchestrator, trace_file_path, total_chunks):
        self.env = env
        self.orchestrator = orchestrator
        self.trace_file_path = trace_file_path
        self.total_chunks = total_chunks

        # 初始化选择的解析器
        parser_options = TRACE_FORMAT_OPTIONS.get(TRACE_FORMAT, {})
        self.parser = get_parser(TRACE_FORMAT, parser_options)

        self.action = env.process(self.run())
        self.requests_generated = 0
        self.latencies = []
        self.completed_requests = 0
        self.chunk_access_log = []


    def _convert_raw_entry_to_sim_values(self, raw_entry: RawTraceEntry):
        """将RawTraceEntry转换为模拟器内部使用的标准化值"""
        # 1. 时间戳转换为毫秒 (ms)
        current_trace_time_ms = 0
        if raw_entry.timestamp_unit == '100ns_windows':
            current_trace_time_ms = self.parser.windows_filetime_to_ms(raw_entry.raw_timestamp)
        elif raw_entry.timestamp_unit == 's': # 秒
            current_trace_time_ms = float(raw_entry.raw_timestamp) * 1000.0
        elif raw_entry.timestamp_unit == 'ms': # 毫秒
            current_trace_time_ms = float(raw_entry.raw_timestamp)
        else:
            raise ValueError(f"Unsupported timestamp unit: {raw_entry.timestamp_unit}")
        if current_trace_time_ms is None: return None # 转换失败

        # 2. 偏移量转换为 LBA
        lba = 0
        raw_offset_val = int(raw_entry.raw_offset) # 假设原始偏移量总是数字
        if raw_entry.offset_unit == 'bytes':
            lba = raw_offset_val // LBA_SIZE_BYTES
        elif raw_entry.offset_unit == 'lba':
            lba = raw_offset_val
        else:
            raise ValueError(f"Unsupported offset unit: {raw_entry.offset_unit}")

        # 3. 大小转换为 bytes
        size_bytes = 0
        raw_size_val = int(raw_entry.raw_size) # 假设原始大小总是数字
        if raw_entry.size_unit == 'bytes':
            size_bytes = raw_size_val
        elif raw_entry.size_unit == 'blocks': # 'blocks' 指的是LBA数量
            size_bytes = raw_size_val * LBA_SIZE_BYTES
        else:
            raise ValueError(f"Unsupported size unit: {raw_entry.size_unit}")

        # 4. 操作类型 (确保是小写 'read'/'write')
        operation_type = raw_entry.operation_type.lower()
        if operation_type not in ['read', 'write']:
            print(f"Warning: Unknown operation type '{raw_entry.operation_type}', defaulting to 'read'.")
            operation_type = 'read' # 或者抛出错误

        return current_trace_time_ms, lba, size_bytes, operation_type


    def run(self):
        print(f"RequestGenerator started at {self.env.now} using parser for format: {TRACE_FORMAT}")
        last_sim_time_ms = 0.0 # 用于计算inter-arrival的模拟时间戳（非trace原始时间戳）
        sim_req_id_counter = 0
        first_request_processed = False

        try:
            with open(self.trace_file_path, 'r') as f:
                for line_num, line_content in enumerate(f, 1):
                    raw_entry = self.parser.parse_line(line_content)
                    if raw_entry is None: # 跳过无效行/头部/注释
                        continue

                    conversion_result = self._convert_raw_entry_to_sim_values(raw_entry)
                    if conversion_result is None:
                        print(f"Skipping line {line_num} due to conversion error: {line_content.strip()}")
                        continue

                    current_trace_time_ms, lba, size_bytes, req_type = conversion_result

                    # 计算模拟中的等待时间
                    sim_wait_time_ms = 0
                    if not first_request_processed:
                        # 第一个请求，以其在trace中的时间作为模拟的起点（或相对起点）
                        # 我们将第一个请求的last_sim_time_ms设为其自身的trace时间，所以等待时间为0
                        last_sim_time_ms = current_trace_time_ms
                        first_request_processed = True
                        # sim_wait_time_ms 保持为 0
                    else:
                        sim_wait_time_ms = current_trace_time_ms - last_sim_time_ms
                        if sim_wait_time_ms < 0:
                            sim_wait_time_ms = 0 # 避免时间倒流
                        last_sim_time_ms = current_trace_time_ms # 更新为当前请求的trace时间

                    if sim_wait_time_ms > 0:
                        yield self.env.timeout(sim_wait_time_ms)

                    sim_req_id_counter += 1
                    request = Request(
                        req_id=sim_req_id_counter,
                        timestamp_orig_raw=raw_entry.raw_timestamp, # 存储最原始的时间戳
                        lba=lba,
                        size_bytes=size_bytes,
                        req_type=req_type,
                        arrival_time_in_sim=self.env.now,
                        hostname=raw_entry.hostname,
                        disk_num=raw_entry.disk_number,
                        orig_response_time_raw=raw_entry.original_response_time,
                        extra_fields=raw_entry.extra_fields
                    )

                    chunk_id, _ = request.get_chunk_id_and_offset()
                    self.chunk_access_log.append((self.env.now, chunk_id, req_type, size_bytes))

                    self.env.process(self.orchestrator.handle_io_request(request))
                    self.requests_generated += 1

                    if SIMULATION_TIME is not None and self.env.now > SIMULATION_TIME:
                        print(f"Simulation time limit ({SIMULATION_TIME} ms) reached in RequestGenerator.")
                        break
        except FileNotFoundError:
            print(f"Error: Trace file not found at {self.trace_file_path}")
        except Exception as e:
            print(f"An unexpected error occurred in RequestGenerator: {e}")
            import traceback
            traceback.print_exc()

        print(f"RequestGenerator finished at {self.env.now}. Total requests generated: {self.requests_generated}")

    def log_completion(self, request: Request):
        request.completion_time_in_sim = self.env.now
        request.latency = request.completion_time_in_sim - request.arrival_time_in_sim
        self.latencies.append(request.latency)
        self.completed_requests += 1