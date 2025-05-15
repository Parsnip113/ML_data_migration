# components/migration_controller.py
# components/migration_controller.py
import simpy
from config import WINDOW_SIZE, SIMULATION_TIME, LOGS_DIR # 确保导入 LOGS_DIR
import os # 新增导入
import time

class MigrationController:
    def __init__(self, env, orchestrator, policy_module, request_generator_ref):
        self.env = env
        self.orchestrator = orchestrator
        self.policy_module = policy_module
        self.request_generator_ref = request_generator_ref
        self.action = env.process(self.run())
        self.last_decision_log_idx = 0

        # --- 日志文件设置 ---
        if not os.path.exists(LOGS_DIR):
            os.makedirs(LOGS_DIR)
        self.log_file_path = os.path.join(LOGS_DIR, "migration_controller.log")
        with open(self.log_file_path, 'w') as f:
            f.write(f"--- MigrationController Log Started at SimTime {self.env.now:.2f} ---\n")
        # --- 日志文件设置结束 ---

    def _log(self, message):
        """辅助方法，用于向特定文件写入日志"""
        with open(self.log_file_path, 'a') as f:
            f.write(f"[MigrationCtrl {self.env.now:.2f}] {message}\n")


    def run(self):
        self._log("Started.")
        while True:
            yield self.env.timeout(WINDOW_SIZE)
            current_time = self.env.now # 在 yield 之后获取，才是当前窗口的决策时间
            self._log(f"Decision window begins at SimTime {current_time:.2f}.")

            current_access_log = self.request_generator_ref.chunk_access_log
            log_for_this_window = current_access_log[self.last_decision_log_idx:]
            self.last_decision_log_idx = len(current_access_log)
            self._log(f"Processing {len(log_for_this_window)} new access records for this window.")

            # 确保 policy_module 存在才调用
            if not self.policy_module:
                self._log("No policy module configured. Skipping migration decisions.")
                migration_decisions = []
            else:
                migration_decisions = self.policy_module.get_migration_decisions(current_time, log_for_this_window)

            if not migration_decisions:
                self._log("No migration tasks received from policy.")
            else:
                self._log(f"Received {len(migration_decisions)} migration tasks from policy: {migration_decisions}")

                evictions = [d for d in migration_decisions if d['action'] == 'evict']
                promotions = [d for d in migration_decisions if d['action'] == 'promote']

                migration_tasks_executed_this_window = 0
                for decision in evictions:
                    self._log(f"Attempting Eviction: Chunk {decision['chunk_id']} from Tier {decision['src_tier_idx']} to Tier {decision['dest_tier_idx']}")
                    migration_success = yield self.env.process(
                        self.orchestrator.execute_migration_command(
                            decision['chunk_id'], decision['src_tier_idx'], decision['dest_tier_idx'], reason="eviction_by_policy"
                        )
                    ) # 传递 reason
                    if migration_success:
                        self._log(f"Eviction SUCCEEDED for chunk {decision['chunk_id']}.")
                        migration_tasks_executed_this_window += 1
                    else:
                        self._log(f"Eviction FAILED for chunk {decision['chunk_id']}.")

                for decision in promotions:
                    self._log(f"Attempting Promotion: Chunk {decision['chunk_id']} from Tier {decision['src_tier_idx']} to Tier {decision['dest_tier_idx']}")
                    migration_success = yield self.env.process(
                        self.orchestrator.execute_migration_command(
                            decision['chunk_id'], decision['src_tier_idx'], decision['dest_tier_idx'], reason="promotion_by_policy"
                        )
                    ) # 传递 reason
                    if migration_success:
                        self._log(f"Promotion SUCCEEDED for chunk {decision['chunk_id']}.")
                        migration_tasks_executed_this_window += 1
                    else:
                        self._log(f"Promotion FAILED for chunk {decision['chunk_id']}.")

                self._log(f"Finished executing {migration_tasks_executed_this_window} migration tasks for this window.")

            if current_time > SIMULATION_TIME and self.request_generator_ref.requests_generated > 0 and \
               self.request_generator_ref.completed_requests >= self.request_generator_ref.requests_generated :
                self._log("Stopping as simulation time ended and requests processed.")
                break
            elif current_time > SIMULATION_TIME * 1.1:
                self._log("Force stopping due to extended simulation time.")
                break
        self._log("Stopped.")