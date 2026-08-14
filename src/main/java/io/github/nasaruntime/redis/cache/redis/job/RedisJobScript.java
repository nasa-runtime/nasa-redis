package io.github.nasaruntime.redis.cache.redis.job;

/**
 * 业务作用：枚举 RedisJob 随 jar 发布并由 SHA 缓存执行的权威状态脚本。
 */
enum RedisJobScript {
    JOB_REGISTER("job_register.lua"),
    JOB_PAUSE("job_pause.lua"),
    JOB_RESUME("job_resume.lua"),
    JOB_DELETE("job_delete.lua"),
    JOB_CLEANUP_TOMBSTONES("job_cleanup_tombstones.lua"),
    JOB_RESOLVE_CONFLICT("job_resolve_conflict.lua"),
    NAMESPACE_SET_STATE("namespace_set_state.lua"),
    SCAN_DUE("scan_due.lua"),
    FANOUT_SCAN_DUE("fanout_scan_due.lua"),
    READ_RUN("read_run.lua"),
    READ_FANOUT_ROOT("read_fanout_root.lua"),
    READ_FANOUT_SHARD("read_fanout_shard.lua"),
    FIRE_DUE("fire_due.lua"),
    FIRE_DUE_BATCH("fire_due_batch.lua"),
    MANUAL_FIRE("manual_fire.lua"),
    START_RUN("start_run.lua"),
    DEFER_RUN("defer_run.lua"),
    RENEW_RUN("renew_run.lua"),
    RENEW_BATCH("renew_batch.lua"),
    FINISH_RUN("finish_run.lua"),
    RECOVER_EXPIRED("recover_expired.lua"),
    PROMOTE_VISIBLE("promote_visible.lua"),
    REQUEST_CANCEL("request_cancel.lua"),
    EXECUTOR_REGISTER("executor_register_capabilities.lua"),
    EXECUTOR_HEARTBEAT("executor_heartbeat.lua"),
    EXECUTOR_UNREGISTER("executor_unregister.lua"),
    EXECUTOR_RECORD_FANOUT_EVIDENCE("executor_record_fanout_evidence.lua"),
    REGISTRY_GC("registry_gc.lua"),
    FANOUT_SNAPSHOT("fanout_snapshot.lua"),
    PREPARE_FANOUT_ROOT("prepare_fanout_root.lua"),
    FINISH_FANOUT_ROOT("finish_fanout_root.lua"),
    FAIL_WAITING_CREATION("fail_waiting_creation.lua"),
    FANOUT_BEGIN("fanout_begin.lua"),
    FANOUT_ADD_SHARDS("fanout_add_shards.lua"),
    FANOUT_COMMIT("fanout_commit.lua"),
    FANOUT_DELIVER_BATCH("fanout_deliver_batch.lua"),
    FANOUT_ACCEPT_SHARD("fanout_accept_shard.lua"),
    FANOUT_RETRY_RECEIPT("fanout_retry_receipt.lua"),
    FANOUT_PROMOTE_READY("fanout_promote_ready.lua"),
    FANOUT_REASSIGN_SHARD("fanout_reassign_shard.lua"),
    FANOUT_AGGREGATE("fanout_aggregate.lua"),
    FANOUT_FAIL_CREATING("fanout_fail_creating.lua"),
    FANOUT_CANCEL_BATCH("fanout_cancel_batch.lua"),
    FANOUT_ADVANCE_CAPABILITY_CURSOR("fanout_advance_capability_cursor.lua"),
    FANOUT_WATCH_ROOT("fanout_watch_root.lua"),
    FANOUT_MARK_RECONCILED("fanout_mark_reconciled.lua"),
    FANOUT_CLEANUP("fanout_cleanup.lua");

    private final String fileName;

    /**
     * 业务作用：绑定脚本枚举与类路径资源名。
     *
     * @param fileName 脚本文件名
     */
    RedisJobScript(String fileName) {
        this.fileName = fileName;
    }

    /**
     * 业务作用：读取类路径脚本文件名。
     *
     * @return 脚本文件名。
     */
    String fileName() {
        return fileName;
    }
}
