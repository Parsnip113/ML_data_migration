# components/trace_parser.py
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import math

@dataclass
class RawTraceEntry:
    """
    解析器从trace行中提取的标准化原始数据结构。
    所有时间戳最终会被转换为毫秒 (ms) 供模拟器使用。
    所有偏移量最终会被转换为 LBA (基于 LBA_SIZE_BYTES)。
    所有大小最终会被转换为字节 (bytes)。
    """
    # 原始时间戳相关
    raw_timestamp: Any # 可以是数字或字符串，具体解析器处理
    timestamp_unit: str # e.g., '100ns_windows', 's' (seconds), 'ms' (milliseconds)

    # 原始偏移量相关
    raw_offset: Any # 可以是数字或字符串
    offset_unit: str # e.g., 'bytes', 'lba'

    # 原始大小相关
    raw_size: Any
    size_unit: str # e.g., 'bytes', 'blocks' (指LBA数量)

    # 操作类型
    operation_type: str # 期望是 'read' or 'write' (小写)

    # 其他可选字段
    hostname: Optional[str] = None
    disk_number: Optional[int] = None
    original_response_time: Optional[Any] = None # 原始trace中的响应时间
    original_response_time_unit: Optional[str] = None # e.g., '100ns_windows'

    extra_fields: Dict[str, Any] = field(default_factory=dict)


class TraceParser(ABC):
    def __init__(self, has_header=False): # 添加 has_header 参数到基类
        self.has_header = has_header
        self.header_processed = not self.has_header # 如果没有header，则认为已处理

    def parse_line(self, line: str) -> Optional[RawTraceEntry]:
        """
        解析单行trace文本。
        如果行无效、是注释或头部，则返回 None。
        否则返回一个 RawTraceEntry 对象。
        """
        if self.has_header and not self.header_processed:
            self.header_processed = True
            # print(f"Skipping header line: {line.strip()}")
            return None
        return self._parse_data_line(line)

    @abstractmethod
    def _parse_data_line(self, line: str) -> Optional[RawTraceEntry]:
        """
        实际解析数据行的逻辑，由子类实现。
        """
        pass

    def windows_filetime_to_ms(self, filetime_val: Any) -> Optional[float]:
        """辅助函数：将Windows filetime (100ns单位) 转换为毫秒"""
        try:
            if isinstance(filetime_val, str):
                try:
                    filetime_int = int(filetime_val)
                except ValueError:
                    filetime_int = int(float(filetime_val)) # 兼容科学计数法
            elif isinstance(filetime_val, (int, float)):
                filetime_int = int(filetime_val)
            else:
                return None
            return filetime_int / 10000.0  # 100ns to 1ms
        except ValueError:
            # print(f"Error converting filetime value: {filetime_val}")
            return None

    def _convert_unix_fractional_s_to_ms_val(self, timestamp_str: str, precision='ms') -> Optional[float]:
        """将秒级带小数的Unix时间戳字符串转换为毫秒，并可选地截断到毫秒整数"""
        try:
            ts_float_seconds = float(timestamp_str)
            ts_float_ms = ts_float_seconds * 1000.0
            if precision == 'ms_int': # 如果需要整数毫秒
                return float(math.floor(ts_float_ms)) # 或者 round(), math.trunc() 取决于需求
            elif precision == 'ms_float': # 保留毫秒级的小数部分
                 return ts_float_ms
            else: # 默认截断到毫秒 (等同于ms_int的效果，因为SimPy的timeout通常用float)
                return float(math.floor(ts_float_ms))
        except ValueError:
            return None

# --- MSR Cambridge Trace Parser ---
class MSRTraceParser(TraceParser):
    def __init__(self):
        super().__init__(has_header=False) # MSR trace 通常没有头部

    def _parse_data_line(self, line: str) -> Optional[RawTraceEntry]:
        parts = line.strip().split(',')
        if len(parts) != 7:
            # print(f"MSRParser: Skipping malformed line (expected 7 parts): {line.strip()}")
            return None

        # Timestamp,Hostname,DiskNumber,Type,Offset,Size,ResponseTime
        raw_ts_str, hostname_str, disk_num_str, type_str, offset_str, size_str, resp_time_str = parts

        try:
            op_type = type_str.lower()
            if op_type not in ['read', 'write']:
                # print(f"MSRParser: Unknown IOType '{type_str}', skipping line: {line.strip()}")
                return None

            return RawTraceEntry(
                raw_timestamp=raw_ts_str,
                timestamp_unit='100ns_windows',
                raw_offset=offset_str,
                offset_unit='bytes',
                raw_size=size_str,
                size_unit='bytes',
                operation_type=op_type,
                hostname=hostname_str,
                disk_number=int(disk_num_str),
                original_response_time=resp_time_str,
                original_response_time_unit='100ns_windows'
            )
        except ValueError as e:
            # print(f"MSRParser: Error converting MSR trace fields: {e} for line: {line.strip()}")
            return None

# --- Systor '17 Trace Parser ---
class Systor17Parser(TraceParser):
    def __init__(self, has_header=True):
        super().__init__(has_header=has_header)

    def _parse_data_line(self, line: str) -> Optional[RawTraceEntry]:
        parts = line.strip().split(',')
        if len(parts) != 6:
            return None

        raw_ts_str, resp_time_str, iotype_str, lun_str, offset_str, size_str = parts

        try:
            # --- 时间戳处理：直接转换为毫秒，并截断 ---
            # Systor '17 的 Timestamp 是 "秒.小数部分"
            # 我们希望 raw_timestamp 存储的是毫秒级的数值
            timestamp_ms = self._convert_unix_fractional_s_to_ms_val(raw_ts_str, precision='ms_float') # 保留小数精度
            if timestamp_ms is None:
                # print(f"Systor17Parser: Invalid timestamp format: {raw_ts_str}")
                return None

            # 响应时间也一样处理
            response_s_float = float(resp_time_str) # 这是持续时间，不是绝对时间点

            op_type_lower = iotype_str.lower()
            if 'read' in op_type_lower or 'r' == op_type_lower: operation_type = 'read'
            elif 'write' in op_type_lower or 'w' == op_type_lower: operation_type = 'write'
            elif iotype_str == "": return None
            else: return None

            return RawTraceEntry(
                raw_timestamp=timestamp_ms, # 直接存储处理后的毫秒值
                timestamp_unit='ms',        # 标记单位为 'ms'
                raw_offset=offset_str, offset_unit='bytes',
                raw_size=size_str, size_unit='bytes',
                operation_type=operation_type,
                disk_number=int(lun_str),
                original_response_time=response_s_float, # 存储秒为单位的响应时间浮点数
                original_response_time_unit='s'          # 标记单位为秒
            )
        except ValueError:
            return None

# # --- Generic CSV Parser (示例：timestamp_ms,lba,size_bytes,type) ---
# class GenericCSVParser(TraceParser):
#     def __init__(self, has_header=True):
#         self.has_header = has_header
#         self.header_processed = False

#     def parse_line(self, line: str) -> Optional[RawTraceEntry]:
#         if self.has_header and not self.header_processed:
#             self.header_processed = True
#             # print(f"GenericCSVParser: Skipping header: {line.strip()}")
#             return None

#         parts = line.strip().split(',')
#         if len(parts) != 4: # 假设4个字段: timestamp_ms,lba,size_bytes,type
#             # print(f"GenericCSVParser: Skipping malformed line (expected 4 parts): {line.strip()}")
#             return None

#         raw_ts_str, lba_str, size_bytes_str, type_str = parts

#         try:
#             return RawTraceEntry(
#                 raw_timestamp=raw_ts_str,
#                 timestamp_unit='ms', # 假设此格式的时间戳已经是毫秒
#                 raw_offset=lba_str,
#                 offset_unit='lba',   # 假设此格式的偏移量是LBA
#                 raw_size=size_bytes_str,
#                 size_unit='bytes', # 假设此格式的大小是字节
#                 operation_type=type_str.lower()
#             )
#         except ValueError as e:
#             # print(f"GenericCSVParser: Error converting fields: {e} for line: {line.strip()}")
#             return None

# # --- Placeholder for Tencent CBS Trace Parser (需要具体格式) ---
# class CBSTraceParser(TraceParser):
#     def __init__(self, has_header=True): # 假设可能有头部
#         self.has_header = has_header
#         self.header_processed = False

#     def parse_line(self, line: str) -> Optional[RawTraceEntry]:
#         if self.has_header and not self.header_processed:
#             self.header_processed = True
#             return None

#         # 假设格式: timestamp(s),offset(bytes),size(bytes),type(read/write),disk_id
#         # 这是基于AIT论文和OSCA论文的猜测，具体分隔符和字段顺序需要确认
#         parts = line.strip().split(',') # 假设是CSV
#         if len(parts) != 5:
#             # print(f"CBSTraceParser: Skipping malformed line (expected 5 parts): {line.strip()}")
#             return None

#         raw_ts_str, offset_str, size_str, type_str, disk_id_str = parts
#         try:
#             return RawTraceEntry(
#                 raw_timestamp=raw_ts_str,
#                 timestamp_unit='s', # 假设时间戳是秒
#                 raw_offset=offset_str,
#                 offset_unit='bytes',
#                 raw_size=size_str,
#                 size_unit='bytes',
#                 operation_type=type_str.lower(),
#                 extra_fields={'disk_id': disk_id_str} # 将额外字段存起来
#             )
#         except ValueError as e:
#             # print(f"CBSTraceParser: Error converting fields: {e} for line: {line.strip()}")
#             return None

def get_parser(trace_format: str, format_options: Optional[Dict] = None) -> TraceParser:
    if format_options is None: format_options = {}
    fmt_upper = trace_format.upper()

    if fmt_upper == "MSR": return MSRTraceParser()
    elif fmt_upper == "SYSTOR17": return Systor17Parser(has_header=format_options.get('has_header', True))
    # elif fmt_upper == "GENERIC_CSV": return GenericCSVParser(has_header=format_options.get('has_header', True))
    # elif fmt_upper == "CBS": return CBSTraceParser(has_header=format_options.get('has_header', True))
    else: raise ValueError(f"Unsupported trace format: {trace_format}")