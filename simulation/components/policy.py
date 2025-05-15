# components/policy.py
# components/policy.py
from abc import ABC, abstractmethod
from config import CHUNK_SIZE_BYTES, LOGS_DIR # 确保导入 LOGS_DIR
import os # 新增导入
import time # 用于时间戳文件名或日志条目

class BasePolicy(ABC):
    def __init__(self, env, orchestrator, tiers, config):
        self.env = env
        self.orchestrator = orchestrator
        self.tiers = tiers
        self.config = config

    @abstractmethod
    def get_migration_decisions(self, current_time, chunk_access_log_since_last_decision):
        pass

class SimpleLFUPolicy(BasePolicy):
    def __init__(self, env, orchestrator, tiers, config):
        super().__init__(env, orchestrator, tiers, config)
        self.chunk_frequencies = {}

        # --- 日志文件设置 ---
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
        self.log_file_path = os.path.join(LOGS_DIR, "policy_SimpleLFU.log")
        # 每次模拟开始时覆盖日志文件
        with open(self.log_file_path, 'w') as f:
            f.write(f"--- SimpleLFUPolicy Log Started at SimTime {self.env.now:.2f} ---\n")
        # --- 日志文件设置结束 ---

    def _log(self, message):
        """辅助方法，用于向特定文件写入日志"""
        with open(self.log_file_path, 'a') as f:
            f.write(f"[Policy {self.env.now:.2f}] {message}\n")

    def get_migration_decisions(self, current_time, chunk_access_log_since_last_decision):
        # 使用 self.env.now 获取当前模拟时间用于日志条目
        self._log(f"--- Evaluating Migration Decisions ---")
        self._log(f"Received {len(chunk_access_log_since_last_decision)} access records for this window.")
        if chunk_access_log_since_last_decision:
            self._log(f"Sample access log (first 5): {chunk_access_log_since_last_decision[:5]}")

        for _, chunk_id, _, _ in chunk_access_log_since_last_decision:
            self.chunk_frequencies[chunk_id] = self.chunk_frequencies.get(chunk_id, 0) + 1

        if self.chunk_frequencies:
            self._log(f"Total unique chunks with frequency info: {len(self.chunk_frequencies)}")
            sorted_chunks_by_freq = sorted(self.chunk_frequencies.items(), key=lambda item: item[1], reverse=True)
            self._log(f"Top 5 most frequent chunks: {sorted_chunks_by_freq[:5]}")
        else:
            self._log(f"No frequency data available.")
            return []

        migrations = []
        tier1 = self.tiers[0]
        tier0_capacity_chunks = tier1.capacity_bytes // CHUNK_SIZE_BYTES

        tier1_chunks_with_freq = []
        for t1_cid in tier1.chunks.keys():
            tier1_chunks_with_freq.append((t1_cid, self.chunk_frequencies.get(t1_cid, 0)))
        tier1_chunks_with_freq.sort(key=lambda x: x[1])

        for chunk_id, freq in sorted_chunks_by_freq:
            current_loc_idx = self.orchestrator.chunk_locations.get(chunk_id)

            if current_loc_idx is None:
                self._log(f"WARNING: Chunk {chunk_id} not found in orchestrator.chunk_locations. Skipping.")
                continue

            if current_loc_idx > 0:
                if tier1.has_chunk(chunk_id):
                    self._log(f"INFO: Chunk {chunk_id} is marked for Tier {current_loc_idx} but already in Tier 1's actual chunks. Skipping promotion.")
                    continue

                if tier1.get_free_space() >= CHUNK_SIZE_BYTES:
                    self._log(f"Decision: Promote chunk {chunk_id} (freq {freq}) from Tier {current_loc_idx} to Tier 0. Tier 1 has space.")
                    migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': 0})
                else:
                    if tier1_chunks_with_freq:
                        evict_candidate_chunk_id, evict_candidate_freq = tier1_chunks_with_freq[0]
                        if evict_candidate_freq < freq :
                            self._log(f"Decision: Tier 1 full. Evict chunk {evict_candidate_chunk_id} (freq {evict_candidate_freq}) from Tier 0 to Tier 1 (dest_tier_idx=1).")
                            migrations.append({'action': 'evict', 'chunk_id': evict_candidate_chunk_id, 'src_tier_idx': 0, 'dest_tier_idx': 1})
                            self._log(f"Decision: Promote chunk {chunk_id} (freq {freq}) from Tier {current_loc_idx} to Tier 0 after eviction.")
                            migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': 0})
                            tier1_chunks_with_freq.pop(0)
                        else:
                            self._log(f"Tier 1 full, but candidate chunk {chunk_id} (freq {freq}) is not hotter than LFU in Tier 1 (chunk {evict_candidate_chunk_id} has freq {evict_candidate_freq}). No promotion.")
                    else:
                        self._log(f"WARNING: Tier 1 full ({tier1.get_free_space()} bytes free) but no chunks found in tier1.chunks to evict. Capacity: {tier1.capacity_bytes} B. ChunkSize: {CHUNK_SIZE_BYTES} B.")
        if migrations:
            self._log(f"Final migration decisions for this window: {migrations}")
        else:
            self._log(f"No migration decisions generated for this window.")
        self._log(f"--- Finished Evaluating Migration Decisions ---")
        return migrations