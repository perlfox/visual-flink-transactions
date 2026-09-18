package com.example.flink.control.model;

/** Configuration submitted from the GUI's Configuration tab when starting the job. */
public class JobConfig {

    // Bounds enforced server-side by clamp() so a request (from the UI or a direct API call)
    // can never push the embedded Flink MiniCluster, which shares this JVM's heap, into exhausting
    // system memory.
    public static final int MIN_MAX_ACCOUNTS = 1;
    public static final int MAX_MAX_ACCOUNTS = 100_000;

    public static final int MIN_TRANSACTIONS_PER_SECOND = 1;
    public static final int MAX_TRANSACTIONS_PER_SECOND = 10_000;

    public static final long MIN_CHECKPOINT_INTERVAL_MS = 1000;

    public static final int MIN_TASK_MEMORY_MB = 512;
    public static final int MAX_TASK_MEMORY_MB = 4096;

    public static final int MIN_PARALLELISM = 1;
    public static final int MAX_PARALLELISM = 8;

    public static final int MIN_RESTART_ATTEMPTS = 0;
    public static final int MAX_RESTART_ATTEMPTS = 10;

    public static final long MIN_RESTART_DELAY_SECONDS = 1;
    public static final long MAX_RESTART_DELAY_SECONDS = 300;

    public int maxAccounts = 4000;
    public long checkpointIntervalMs = 60000;
    public boolean stateBloat = false;
    public boolean webhookEnabled = false;
    public String webhookUrl = "https://webhook.site/a1b731f8-6003-41f0-948a-6dd9c8c3fa3f";
    public int maxTransactionsPerSecond = 1000;
    public int taskManagerMemoryMb = 512;
    public int parallelism = 2;
    public String checkpointingMode = "EXACTLY_ONCE";
    public int restartAttempts = 3;
    public long restartDelaySeconds = 10;

    /** Clamps every field to a safe range in place; called before a config is used to start a job. */
    public void clamp() {
        maxAccounts = clampInt(maxAccounts, MIN_MAX_ACCOUNTS, MAX_MAX_ACCOUNTS);
        maxTransactionsPerSecond = clampInt(maxTransactionsPerSecond, MIN_TRANSACTIONS_PER_SECOND, MAX_TRANSACTIONS_PER_SECOND);
        taskManagerMemoryMb = clampInt(taskManagerMemoryMb, MIN_TASK_MEMORY_MB, MAX_TASK_MEMORY_MB);
        parallelism = clampInt(parallelism, MIN_PARALLELISM, MAX_PARALLELISM);
        restartAttempts = clampInt(restartAttempts, MIN_RESTART_ATTEMPTS, MAX_RESTART_ATTEMPTS);
        if (restartDelaySeconds < MIN_RESTART_DELAY_SECONDS) {
            restartDelaySeconds = MIN_RESTART_DELAY_SECONDS;
        } else if (restartDelaySeconds > MAX_RESTART_DELAY_SECONDS) {
            restartDelaySeconds = MAX_RESTART_DELAY_SECONDS;
        }
        if (!"AT_LEAST_ONCE".equals(checkpointingMode)) {
            checkpointingMode = "EXACTLY_ONCE";
        }
        if (checkpointIntervalMs < MIN_CHECKPOINT_INTERVAL_MS) {
            checkpointIntervalMs = MIN_CHECKPOINT_INTERVAL_MS;
        }
    }

    private static int clampInt(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
