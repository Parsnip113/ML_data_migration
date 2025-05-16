# components/policy.py
# components/policy.py
from abc import ABC, abstractmethod
from config import CHUNK_SIZE_BYTES, LOGS_DIR , TOTAL_CHUNKS# 确保导入 LOGS_DIR
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



class Migration_more_LFUPolicy(BasePolicy):
    # 迁移策略过于激进， 迁移本身有对设备的读写延迟，同时这段时间占据设备，进而增加IO请求的延迟
    # 当前模拟环境 IO请求 和 数据迁移 平等竞争设备
    # 平均延迟能高达 10ms+
    # 即使所有IO请求访问tier3也不过4 - 5 ms而已
    def __init__(self, env, orchestrator, tiers, config):
        super().__init__(env, orchestrator, tiers, config)
        self.chunk_frequencies = {} # Global accumulated frequencies

        # Define tier indices for clarity (0:fastest, 1:middle, 2:slowest)
        self.TIER_0_IDX = 0
        self.TIER_1_IDX = 1
        self.TIER_2_IDX = 2

        if len(self.tiers) != 3:
            # Log a warning if not exactly 3 tiers, as this policy is tailored for it.
            # The policy might still function but may not be optimal.
            self._log(f"WARNING: SimpleLFUPolicy is designed for 3 tiers, but {len(self.tiers)} tiers are configured.")
            # Adjust tier indices if necessary, or make the policy more generic in a future version.
            # For now, we'll assume the indices 0, 1, 2 are valid if len(tiers) >= 3.
            # If len(tiers) < 2, TIER_1_IDX or TIER_2_IDX might be invalid.
            if len(self.tiers) < 1:
                 raise ValueError("Policy initialized with no tiers.")
            if len(self.tiers) < 2 and self.TIER_1_IDX == 1: # If only 1 tier, TIER_1_IDX is invalid
                self.TIER_1_IDX = -1 # Mark as invalid
                self.TIER_2_IDX = -1
            if len(self.tiers) < 3 and self.TIER_2_IDX == 2: # If only 2 tiers, TIER_2_IDX is invalid
                self.TIER_2_IDX = -1


        # --- 日志文件设置 ---
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)

        # Ensure LOGS_DIR is a directory
        log_file_name = "policy_SimpleLFU.log"
        if os.path.isfile(LOGS_DIR):
            print(f"ERROR: LOGS_DIR '{LOGS_DIR}' is a file, expected a directory. Defaulting log to current dir.")
            self.log_file_path = log_file_name
        else:
            self.log_file_path = os.path.join(LOGS_DIR, log_file_name)

        try:
            with open(self.log_file_path, 'w') as f:
                f.write(f"--- SimpleLFUPolicy Log Started at SimTime {self.env.now:.2f} ---\n")
                f.write(f"Policy configured for {len(self.tiers)} tiers. TIER_0={self.TIER_0_IDX}, TIER_1={self.TIER_1_IDX}, TIER_2={self.TIER_2_IDX}\n")
        except IOError as e:
            print(f"Error initializing policy log file {self.log_file_path}: {e}")


    def _log(self, message):
        """辅助方法，用于向特定文件写入日志"""
        try:
            with open(self.log_file_path, 'a') as f:
                f.write(f"[Policy {self.env.now:.2f}] {message}\n")
        except IOError as e:
            print(f"Error writing to policy log {self.log_file_path}: {e}")

    def get_migration_decisions(self, current_time, chunk_access_log_since_last_decision):
        self._log(f"--- Evaluating Migration Decisions ---")
        self._log(f"Received {len(chunk_access_log_since_last_decision)} access records for this window.")

        # 1. Update global chunk frequencies
        for _, chunk_id, _, _ in chunk_access_log_since_last_decision:
            if 0 <= chunk_id < TOTAL_CHUNKS:
                self.chunk_frequencies[chunk_id] = self.chunk_frequencies.get(chunk_id, 0) + 1
            else:
                self._log(f"WARNING: Invalid chunk_id {chunk_id} in access log. Max expected: {TOTAL_CHUNKS-1}. Skipping.")

        if not self.chunk_frequencies:
            self._log(f"No frequency data available. No migration decisions.")
            return []

        valid_chunk_frequencies = {cid: freq for cid, freq in self.chunk_frequencies.items() if 0 <= cid < TOTAL_CHUNKS}
        sorted_chunks_by_global_freq = sorted(valid_chunk_frequencies.items(), key=lambda item: item[1], reverse=True)

        if not sorted_chunks_by_global_freq:
            self._log("No valid chunks with frequency data. No migrations.")
            return []

        self._log(f"Top 5 most frequent chunks globally: {sorted_chunks_by_global_freq[:5]}")

        migrations = []
        # Keep track of chunks whose migration is already decided in this cycle
        # chunk_id -> target_tier_idx
        pending_chunk_destinations = {}


        # --- Pass 1: Decisions for Tier 0 (Fastest Tier) ---
        self._log(f"--- Pass 1: Decisions for Tier {self.TIER_0_IDX} ---")
        tier0 = self.tiers[self.TIER_0_IDX]

        # Get current occupants of Tier 0 and their frequencies (coldest first)
        tier0_occupants_cids = [
            cid for cid, loc_idx in self.orchestrator.chunk_locations.items()
            if loc_idx == self.TIER_0_IDX and 0 <= cid < TOTAL_CHUNKS
        ]
        tier0_lfu_candidates = sorted(
            [(cid, self.chunk_frequencies.get(cid, 0)) for cid in tier0_occupants_cids],
            key=lambda x: x[1] # Sort by frequency, ascending (coldest first)
        )
        self._log(f"Tier {self.TIER_0_IDX} LFU candidates (cid, freq): {tier0_lfu_candidates[:5]}")

        for chunk_id, freq in sorted_chunks_by_global_freq:
            current_loc_idx = self.orchestrator.chunk_locations.get(chunk_id)

            if current_loc_idx is None:
                self._log(f"  SKIP Tier0 promote: Chunk {chunk_id} not in orchestrator.chunk_locations.")
                continue
            if current_loc_idx == self.TIER_0_IDX: # Already in the fastest tier
                continue
            if pending_chunk_destinations.get(chunk_id) == self.TIER_0_IDX: # Already decided to promote here
                continue

            self._log(f"  Considering for Tier {self.TIER_0_IDX}: Chunk {chunk_id} (freq {freq}, current Tier {current_loc_idx})")

            # Calculate current free space in Tier 0, accounting for pending evictions *from* Tier 0
            # and pending promotions *to* Tier 0. This is complex for a simple LFU.
            # Simple approach: use current free space. Orchestrator handles actual space.
            # A slightly better approach: count how many promotions to tier0 we've already decided.
            num_pending_promotions_to_tier0 = sum(1 for dest_tier in pending_chunk_destinations.values() if dest_tier == self.TIER_0_IDX)
            effective_free_chunks_tier0 = (tier0.get_free_space() // CHUNK_SIZE_BYTES) - num_pending_promotions_to_tier0


            if effective_free_chunks_tier0 > 0 : # Tier 0 has space
                self._log(f"    DECISION (Promote to Tier {self.TIER_0_IDX}): Chunk {chunk_id} from Tier {current_loc_idx}. Tier {self.TIER_0_IDX} has space.")
                migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': self.TIER_0_IDX})
                pending_chunk_destinations[chunk_id] = self.TIER_0_IDX
            else: # Tier 0 is full (or considered full by pending promotions)
                if tier0_lfu_candidates:
                    lfu_evict_candidate_id, lfu_evict_candidate_freq = tier0_lfu_candidates[0]

                    # Ensure LFU candidate is not already planned for promotion elsewhere or is the current hot chunk
                    if pending_chunk_destinations.get(lfu_evict_candidate_id) is not None and \
                       pending_chunk_destinations.get(lfu_evict_candidate_id) != self.TIER_1_IDX : # if its going somewhere other than tier 1
                        self._log(f"    SKIP Tier0 Eviction: LFU Chunk {lfu_evict_candidate_id} in Tier {self.TIER_0_IDX} is already pending migration to {pending_chunk_destinations.get(lfu_evict_candidate_id)}. Find next LFU.")
                        tier0_lfu_candidates.pop(0) # Remove it and try next one in next iteration if any
                        if not tier0_lfu_candidates:
                            self._log(f"    BLOCK Tier0 Promote: Tier {self.TIER_0_IDX} full, no more valid LFU candidates to evict for Chunk {chunk_id}.")
                            break # Stop trying to promote to Tier 0 if no more LFU can be evicted
                        continue # Re-evaluate with the new LFU head if loop continues

                    if freq > lfu_evict_candidate_freq:
                        self._log(f"    Tier {self.TIER_0_IDX} full. Candidate Chunk {chunk_id} (freq {freq}) is hotter than LFU Chunk {lfu_evict_candidate_id} (freq {lfu_evict_candidate_freq}) in Tier {self.TIER_0_IDX}.")
                        if self.TIER_1_IDX != -1: # Check if Tier 1 is valid
                            self._log(f"      DECISION (Evict from Tier {self.TIER_0_IDX}): LFU Chunk {lfu_evict_candidate_id} to Tier {self.TIER_1_IDX}.")
                            migrations.append({'action': 'evict', 'chunk_id': lfu_evict_candidate_id, 'src_tier_idx': self.TIER_0_IDX, 'dest_tier_idx': self.TIER_1_IDX})
                            pending_chunk_destinations[lfu_evict_candidate_id] = self.TIER_1_IDX
                            tier0_lfu_candidates.pop(0) # Removed from LFU list for this cycle

                            self._log(f"      DECISION (Promote to Tier {self.TIER_0_IDX}): Chunk {chunk_id} from Tier {current_loc_idx} after eviction.")
                            migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': self.TIER_0_IDX})
                            pending_chunk_destinations[chunk_id] = self.TIER_0_IDX
                        else:
                            self._log(f"    BLOCK Tier0 Promote: Tier {self.TIER_0_IDX} full, LFU Chunk {lfu_evict_candidate_id} needs eviction, but no valid Tier {self.TIER_1_IDX} to evict to.")
                            break # Stop trying to promote if no valid eviction target
                    else:
                        self._log(f"    BLOCK Tier0 Promote: Tier {self.TIER_0_IDX} full. Candidate Chunk {chunk_id} (freq {freq}) not hotter than LFU {lfu_evict_candidate_id} (freq {lfu_evict_candidate_freq}).")
                        break # Current hottest chunk can't get in, so colder ones won't either.
                else: # Tier 0 is full but no LFU candidates (e.g. all its chunks are somehow pending migration)
                    self._log(f"    BLOCK Tier0 Promote: Tier {self.TIER_0_IDX} full, but no LFU candidates available for eviction for Chunk {chunk_id}.")
                    break # Stop trying to promote to Tier 0

        # --- Pass 2: Decisions for Tier 1 (Middle Tier), promoting from Tier 2 ---
        if self.TIER_1_IDX == -1 or self.TIER_2_IDX == -1: # Skip if not a 3-tier setup or more
            self._log(f"--- Skipping Pass 2 (Tier 1 decisions) due to tier configuration. ---")
        else:
            self._log(f"--- Pass 2: Decisions for Tier {self.TIER_1_IDX} (from Tier {self.TIER_2_IDX}) ---")
            tier1 = self.tiers[self.TIER_1_IDX]

            # Get current occupants of Tier 1, EXCLUDING those just decided for promotion to Tier 0,
            # BUT INCLUDING those just decided for eviction from Tier 0 (now notionally in Tier 1).
            tier1_effective_occupants_cids = []
            for cid_loop in range(TOTAL_CHUNKS):
                current_loc = self.orchestrator.chunk_locations.get(cid_loop)
                pending_dest = pending_chunk_destinations.get(cid_loop)

                if pending_dest == self.TIER_1_IDX: # Chunk is moving to Tier 1 (e.g., evicted from Tier 0)
                    tier1_effective_occupants_cids.append(cid_loop)
                elif pending_dest is None and current_loc == self.TIER_1_IDX: # Chunk is in Tier 1 and not moving out
                    tier1_effective_occupants_cids.append(cid_loop)
                # Chunks pending to Tier 0 or other tiers are not considered occupants of Tier 1 for LFU.

            tier1_lfu_candidates = sorted(
                [(cid, self.chunk_frequencies.get(cid, 0)) for cid in tier1_effective_occupants_cids],
                key=lambda x: x[1] # Sort by frequency, ascending (coldest first)
            )
            self._log(f"Tier {self.TIER_1_IDX} effective LFU candidates (cid, freq): {tier1_lfu_candidates[:5]}")

            for chunk_id, freq in sorted_chunks_by_global_freq:
                current_loc_idx = self.orchestrator.chunk_locations.get(chunk_id)

                if current_loc_idx is None:
                    self._log(f"  SKIP Tier1 promote: Chunk {chunk_id} not in orchestrator.chunk_locations.")
                    continue

                # Skip if chunk is already in Tier 0 or Tier 1, or already decided for Tier 0 or Tier 1
                if current_loc_idx == self.TIER_0_IDX or current_loc_idx == self.TIER_1_IDX:
                    continue
                if pending_chunk_destinations.get(chunk_id) == self.TIER_0_IDX or \
                   pending_chunk_destinations.get(chunk_id) == self.TIER_1_IDX:
                    continue

                # At this point, chunk_id is in TIER_2_IDX and not pending migration to T0 or T1
                self._log(f"  Considering for Tier {self.TIER_1_IDX}: Chunk {chunk_id} (freq {freq}, current Tier {current_loc_idx})")

                num_pending_promotions_to_tier1 = sum(1 for dest_tier in pending_chunk_destinations.values() if dest_tier == self.TIER_1_IDX)
                # We also need to account for chunks evicted from Tier 0 to Tier 1 (already counted if pending_dest == TIER_1_IDX)
                # and chunks promoted from Tier 1 to Tier 0 (which free up space).
                # This space calculation is tricky. Let's use current free space for simplicity first.
                # A more accurate effective_free_chunks_tier1 would be:
                # (tier1.get_free_space() // CHUNK_SIZE_BYTES)
                #   - (count of new promotions to T1 from T2 in *this pass*)
                #   + (count of promotions from T1 to T0 in *Pass 1*)
                #   - (count of evictions from T0 to T1 in *Pass 1* if not already counted by effective_occupants)

                # Simplified space check for Tier 1 for now:
                effective_free_chunks_tier1 = (tier1.get_free_space() // CHUNK_SIZE_BYTES) - \
                                              sum(1 for cid, dest_tier in pending_chunk_destinations.items()
                                                  if dest_tier == self.TIER_1_IDX and self.orchestrator.chunk_locations.get(cid) != self.TIER_0_IDX)
                                              # Subtract promotions to T1 from T2, don't double count evictions from T0.

                if effective_free_chunks_tier1 > 0: # Tier 1 has space
                    self._log(f"    DECISION (Promote to Tier {self.TIER_1_IDX}): Chunk {chunk_id} from Tier {current_loc_idx}. Tier {self.TIER_1_IDX} has space.")
                    migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': self.TIER_1_IDX})
                    pending_chunk_destinations[chunk_id] = self.TIER_1_IDX
                else: # Tier 1 is full
                    if tier1_lfu_candidates:
                        lfu1_evict_candidate_id, lfu1_evict_candidate_freq = tier1_lfu_candidates[0]

                        # Ensure LFU candidate is not already planned for promotion to Tier 0
                        if pending_chunk_destinations.get(lfu1_evict_candidate_id) == self.TIER_0_IDX:
                            self._log(f"    SKIP Tier1 Eviction: LFU Chunk {lfu1_evict_candidate_id} in Tier {self.TIER_1_IDX} is already pending promotion to Tier {self.TIER_0_IDX}. Find next LFU.")
                            tier1_lfu_candidates.pop(0)
                            if not tier1_lfu_candidates:
                                self._log(f"    BLOCK Tier1 Promote: Tier {self.TIER_1_IDX} full, no more valid LFU candidates to evict for Chunk {chunk_id}.")
                                break
                            continue # Re-evaluate

                        if freq > lfu1_evict_candidate_freq:
                            self._log(f"    Tier {self.TIER_1_IDX} full. Candidate Chunk {chunk_id} (freq {freq}) is hotter than LFU Chunk {lfu1_evict_candidate_id} (freq {lfu1_evict_candidate_freq}) in Tier {self.TIER_1_IDX}.")
                            if self.TIER_2_IDX != -1: # Check if Tier 2 is valid
                                self._log(f"      DECISION (Evict from Tier {self.TIER_1_IDX}): LFU Chunk {lfu1_evict_candidate_id} to Tier {self.TIER_2_IDX}.")
                                migrations.append({'action': 'evict', 'chunk_id': lfu1_evict_candidate_id, 'src_tier_idx': self.TIER_1_IDX, 'dest_tier_idx': self.TIER_2_IDX})
                                pending_chunk_destinations[lfu1_evict_candidate_id] = self.TIER_2_IDX
                                tier1_lfu_candidates.pop(0)

                                self._log(f"      DECISION (Promote to Tier {self.TIER_1_IDX}): Chunk {chunk_id} from Tier {current_loc_idx} after eviction.")
                                migrations.append({'action': 'promote', 'chunk_id': chunk_id, 'src_tier_idx': current_loc_idx, 'dest_tier_idx': self.TIER_1_IDX})
                                pending_chunk_destinations[chunk_id] = self.TIER_1_IDX
                            else:
                                self._log(f"    BLOCK Tier1 Promote: Tier {self.TIER_1_IDX} full, LFU Chunk {lfu1_evict_candidate_id} needs eviction, but no valid Tier {self.TIER_2_IDX} to evict to.")
                                break
                        else:
                            self._log(f"    BLOCK Tier1 Promote: Tier {self.TIER_1_IDX} full. Candidate Chunk {chunk_id} (freq {freq}) not hotter than LFU {lfu1_evict_candidate_id} (freq {lfu1_evict_candidate_freq}).")
                            break
                    else: # Tier 1 is full but no LFU candidates
                        self._log(f"    BLOCK Tier1 Promote: Tier {self.TIER_1_IDX} full, but no LFU candidates available for eviction for Chunk {chunk_id}.")
                        break

        if migrations:
            self._log(f"Final migration decisions for this window ({len(migrations)} tasks): {migrations}")
        else:
            self._log(f"No migration decisions generated for this window.")
        self._log(f"--- Finished Evaluating Migration Decisions ---")
        return migrations
