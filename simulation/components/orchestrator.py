# components/orchestrator.py
import simpy
from config import TOTAL_CHUNKS, CHUNK_SIZE_BYTES, LOGS_DIR # 确保导入 LOGS_DIR
import os # 新增导入
import time

class Orchestrator:
    def __init__(self, env, tiers, request_generator_ref=None):
        self.env = env
        self.tiers = tiers
        self.request_generator_ref = request_generator_ref
        self.chunk_locations = {i: len(tiers) - 1 for i in range(TOTAL_CHUNKS)}

        # --- 日志文件设置 ---
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
        self.log_file_path = os.path.join(LOGS_DIR, "orchestrator.log")
        with open(self.log_file_path, 'w') as f:
            f.write(f"--- Orchestrator Log Started at SimTime {self.env.now:.2f} ---\n")
        # --- 日志文件设置结束 ---

        self.initialization_process = env.process(self._initialize_bottom_tier_chunks_instant())
        self.io_queue = simpy.Store(env)
        self.migration_queue = simpy.Store(env)


    def _log(self, message):
        """辅助方法，用于向特定文件写入日志"""
        with open(self.log_file_path, 'a') as f:
            f.write(f"[Orchestrator {self.env.now:.2f}] {message}\n")

    def _initialize_bottom_tier_chunks_instant(self):
        self._log(f"Starting initial (instant) population of bottom tier metadata...")
        bottom_tier_idx = len(self.tiers) - 1
        bottom_tier = self.tiers[bottom_tier_idx]
        for chunk_id in self.chunk_locations:
            if self.chunk_locations[chunk_id] == bottom_tier_idx:
                success = bottom_tier._add_initial_chunk_metadata(chunk_id, is_dirty=False)
                if not success:
                    self._log(f"CRITICAL ERROR during initial population of chunk {chunk_id} in {bottom_tier.name}")
        self._log(f"Finished initial (instant) population of bottom tier metadata.")
        yield self.env.timeout(0)

    def set_request_generator(self, rg_ref):
        self.request_generator_ref = rg_ref

    def get_chunk_location_tier(self, chunk_id):
        # ... (不变)
        tier_idx = self.chunk_locations.get(chunk_id)
        if tier_idx is not None:
            return self.tiers[tier_idx]
        return None

    def handle_io_request(self, request):
        # ... (这个方法中的 print 暂时可以保留在终端，或者您也可以选择将其写入 orchestrator.log)
        # 如果要写入日志，取消下面一行的注释，并替换掉其他 print
        # self._log(f"Handling IO Req ID {request.id} for LBA {request.lba}")
        current_time = self.env.now # 在 Orchestrator 中定义 log_prefix 不是实例变量，所以这里重新获取
        # ... (原有的 handle_io_request 逻辑，如果需要详细日志，可以将内部print改为self._log)
        chunk_id, _ = request.get_chunk_id_and_offset()
        target_tier_idx = -1
        for i, tier in enumerate(self.tiers):
            if tier.has_chunk(chunk_id):
                target_tier_idx = i
                break

        if target_tier_idx == -1:
            # print(f"[Orchestrator {current_time:.2f}] CRITICAL ERROR: Chunk {chunk_id} (LBA {request.lba}) not found in any tier!") # 保持这个重要错误在终端
            if self.request_generator_ref:
                 self.request_generator_ref.log_completion(request)
            return

        target_tier = self.tiers[target_tier_idx]
        device = target_tier.get_device()
        with device.resource.request() as dev_req:
            yield dev_req
            yield self.env.process(device.access(request.size_bytes, request.req_type))

        if request.req_type == 'write':
            current_chunk_meta = target_tier.get_chunk_meta(chunk_id)
            if current_chunk_meta:
                current_chunk_meta['dirty'] = True

        if self.request_generator_ref:
            self.request_generator_ref.log_completion(request)


    def execute_migration_command(self, chunk_id, src_tier_idx, dest_tier_idx, is_eviction_for_new_chunk=False, reason="unknown"): # 添加 reason
        current_time = self.env.now # 获取当前模拟时间
        self._log(f"Executing Migration (Reason: {reason}): Chunk {chunk_id} from Tier {src_tier_idx} to Tier {dest_tier_idx}")

        if not (0 <= src_tier_idx < len(self.tiers) and 0 <= dest_tier_idx < len(self.tiers)):
            self._log(f"ERROR: Invalid tier index for migration. Src: {src_tier_idx}, Dest: {dest_tier_idx}. Aborting.")
            return False

        src_tier = self.tiers[src_tier_idx]
        dest_tier = self.tiers[dest_tier_idx]

        if self.chunk_locations.get(chunk_id) != src_tier_idx:
            self._log(f"ERROR: Chunk {chunk_id} location mismatch. Orchestrator believes Tier {self.chunk_locations.get(chunk_id)}, migration from Tier {src_tier_idx}. Aborting.")
            return False

        if not src_tier.has_chunk(chunk_id):
            self._log(f"ERROR: Chunk {chunk_id} not found in {src_tier.name}'s internal state. Aborting.")
            return False

        is_moving_to_backing_store = (dest_tier_idx == len(self.tiers) - 1)
        if not is_moving_to_backing_store:
            required_space = CHUNK_SIZE_BYTES
            free_space = dest_tier.get_free_space()
            self._log(f"Dest Tier {dest_tier.name} (idx {dest_tier_idx}): Free space {free_space} B, Required {required_space} B.")
            if free_space < required_space:
                self._log(f"Migration FAILED: Dest Tier {dest_tier.name} has NO SPACE for chunk {chunk_id}.")
                return False

        self._log(f"Removing chunk {chunk_id} from {src_tier.name}...")
        chunk_meta = src_tier.remove_chunk(chunk_id)
        if chunk_meta is None:
            self._log(f"Migration FAILED: Chunk {chunk_id} could not be removed from {src_tier.name} (not found internally).")
            return False
        self._log(f"Chunk {chunk_id} removed from {src_tier.name}. Meta: {chunk_meta}")
        is_dirty = chunk_meta['dirty']

        if is_moving_to_backing_store and not is_dirty and src_tier_idx < dest_tier_idx:
            self._log(f"Clean chunk {chunk_id} evicted to backing store {dest_tier.name}. No physical write to dest.")
            # self._log(f"Ensuring chunk {chunk_id} metadata is present in {dest_tier.name} (as clean).")
            # dest_tier._add_initial_chunk_metadata(chunk_id, is_dirty=False) # 这一步要小心，如果backing store本来就应该有所有块的元数据的话
            # 确保 backing store 真的有这个块的元数据（它应该一直有）
            if not dest_tier.has_chunk(chunk_id): # 如果由于某种原因它不在，则添加（作为初始存在）
                dest_tier._add_initial_chunk_metadata(chunk_id, is_dirty=False)
            else: # 如果在，确保它是干净的
                dest_tier_meta = dest_tier.get_chunk_meta(chunk_id)
                if dest_tier_meta: dest_tier_meta['dirty'] = False

            self.chunk_locations[chunk_id] = dest_tier_idx
            self._log(f"Migration SUCCEEDED (logical for clean chunk {chunk_id} to backing store). Location updated to Tier {dest_tier_idx}.")
            return True

        self._log(f"Writing chunk {chunk_id} to {dest_tier.name} (is_dirty for dest: {is_dirty if not is_moving_to_backing_store else False})...")
        write_is_dirty_for_dest = is_dirty if not is_moving_to_backing_store else False
        write_successful = yield self.env.process(dest_tier.write_chunk(chunk_id, is_dirty=write_is_dirty_for_dest))

        if not write_successful:
            self._log(f"Migration FAILED: Could not write chunk {chunk_id} to {dest_tier.name}.")
            self._log(f"Attempting rollback: writing chunk {chunk_id} back to {src_tier.name}...")
            rollback_success = yield self.env.process(src_tier.write_chunk(chunk_id, is_dirty=is_dirty)) # 使用原始is_dirty状态
            if rollback_success:
                 self._log(f"Rollback successful. Chunk {chunk_id} restored to {src_tier.name}.")
            else:
                 self._log(f"CRITICAL ERROR: Rollback FAILED for chunk {chunk_id} to {src_tier.name}. Data state inconsistent!")
            return False

        self.chunk_locations[chunk_id] = dest_tier_idx
        self._log(f"Migration SUCCEEDED for chunk {chunk_id}. New location: Tier {dest_tier_idx} in {dest_tier.name}.")
        return True