package com.example.flink.control.service;

import com.example.flink.control.model.JobConfig;
import com.example.flink.control.model.JobStatus;
import com.example.flink.control.model.JobStatusResponse;
import com.example.flink.job.TransactionEventBus;
import com.example.flink.job.TransactionProcessor;
import com.example.flink.job.TransactionSourceControl;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Owns the lifecycle of the embedded (local MiniCluster) Flink job: start, stop, and current status.
 * Only one run is active at a time; starting again clears LiveDataStore for a fresh view.
 */
@Service
public class FlinkJobManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobManager.class);
    private static final String JOB_NAME = "Transaction Processor with State";

    private final LiveDataStore liveDataStore;
    private final TransactionEventBus.TransactionListener eventListener;
    private final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "flink-job-lifecycle");
        t.setDaemon(true);
        return t;
    });

    private volatile JobStatus status = JobStatus.STOPPED;
    private volatile JobConfig currentConfig = new JobConfig();
    private volatile Long startedAtEpochMs;
    private volatile String errorMessage;
    private volatile JobClient jobClient;

    public FlinkJobManager(LiveDataStore liveDataStore) {
        this.liveDataStore = liveDataStore;
        this.eventListener = liveDataStore::record;
    }

    public synchronized void start(JobConfig config) {
        if (status != JobStatus.STOPPED && status != JobStatus.FAILED) {
            throw new IllegalStateException("Job is already " + status);
        }
        config.clamp();
        status = JobStatus.STARTING;
        errorMessage = null;
        currentConfig = config;
        liveDataStore.reset();

        lifecycleExecutor.submit(() -> launch(config));
    }

    /** Builds and starts a fresh pipeline. Runs on the lifecycle executor; assumes state is already STARTING. */
    private void launch(JobConfig config) {
        try {
            // A prior run may have been left paused mid-interrogation; every fresh run starts unpaused.
            TransactionSourceControl.resume();

            // Bounds the MiniCluster's own memory pools (managed memory, network buffers, JVM
            // overhead) instead of letting Flink size them off whatever the host machine has free.
            Configuration flinkConfig = new Configuration();
            flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(config.taskManagerMemoryMb));

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config.parallelism, flinkConfig);
            env.enableCheckpointing(config.checkpointIntervalMs);
            env.getCheckpointConfig().setCheckpointingMode(
                    "AT_LEAST_ONCE".equals(config.checkpointingMode) ? CheckpointingMode.AT_LEAST_ONCE : CheckpointingMode.EXACTLY_ONCE);
            env.setRestartStrategy(config.restartAttempts <= 0
                    ? RestartStrategies.noRestart()
                    : RestartStrategies.fixedDelayRestart(config.restartAttempts, Time.seconds(config.restartDelaySeconds)));

            TransactionEventBus.subscribe(eventListener);
            TransactionProcessor.buildPipeline(env, config.maxAccounts, config.stateBloat,
                    config.webhookEnabled, config.webhookUrl, config.maxTransactionsPerSecond);

            jobClient = env.executeAsync(JOB_NAME);
            startedAtEpochMs = System.currentTimeMillis();
            status = JobStatus.RUNNING;
            LOG.info("Flink job started: {}", config);
        } catch (Exception e) {
            LOG.error("Failed to start Flink job", e);
            TransactionEventBus.unsubscribe(eventListener);
            errorMessage = e.getMessage();
            status = JobStatus.FAILED;
        }
    }

    public synchronized void stop() {
        if (status != JobStatus.RUNNING) {
            throw new IllegalStateException("Job is not running (status=" + status + ")");
        }
        status = JobStatus.STOPPING;
        JobClient client = jobClient;

        lifecycleExecutor.submit(() -> {
            try {
                if (client != null) {
                    client.cancel().get(30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                LOG.warn("Error while cancelling Flink job", e);
                errorMessage = "Error while stopping: " + e.getMessage();
            } finally {
                TransactionEventBus.unsubscribe(eventListener);
                jobClient = null;
                startedAtEpochMs = null;
                status = JobStatus.STOPPED;
                LOG.info("Flink job stopped");
            }
        });
    }

    /**
     * Resets all accumulated data: live feed, aggregates, and account balances.
     * If a job is running, its account-balance state lives inside Flink's keyed state, so the only
     * way to truly zero every account is to cancel the current run and start a fresh one with the
     * same config; otherwise this just clears the leftover data from the last run.
     */
    public synchronized void clearData() {
        if (status == JobStatus.STARTING || status == JobStatus.STOPPING) {
            throw new IllegalStateException("Job is currently " + status + "; try again shortly");
        }
        if (status != JobStatus.RUNNING) {
            liveDataStore.reset();
            return;
        }

        JobConfig config = currentConfig;
        JobClient client = jobClient;
        status = JobStatus.STARTING;
        errorMessage = null;

        lifecycleExecutor.submit(() -> {
            try {
                if (client != null) {
                    client.cancel().get(30, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                LOG.warn("Error while cancelling Flink job during data clear", e);
            } finally {
                TransactionEventBus.unsubscribe(eventListener);
                jobClient = null;
                startedAtEpochMs = null;
            }

            liveDataStore.reset();
            launch(config);
        });
    }

    /**
     * Persists a config server-side without starting a job, so the UI can save settings (and have
     * them survive status polls) ahead of clicking Start. Rejected while a job is running/transitioning,
     * since that config is already live and shouldn't be silently swapped out from under it.
     */
    public synchronized JobStatusResponse updateConfig(JobConfig config) {
        if (status == JobStatus.RUNNING || status == JobStatus.STARTING || status == JobStatus.STOPPING) {
            throw new IllegalStateException("Cannot save configuration while job is " + status);
        }
        config.clamp();
        currentConfig = config;
        return statusResponse();
    }

    /**
     * Freezes the mock source (no new transactions) so the Live Feed's "Interrogate" mode can hold
     * a transaction on screen without it scrolling away. The job itself keeps running.
     */
    public synchronized void pauseGeneration() {
        if (status != JobStatus.RUNNING) {
            throw new IllegalStateException("Job is not running (status=" + status + ")");
        }
        TransactionSourceControl.pause();
    }

    public void resumeGeneration() {
        TransactionSourceControl.resume();
    }

    public boolean isGenerationPaused() {
        return TransactionSourceControl.isPaused();
    }

    public JobStatusResponse statusResponse() {
        return new JobStatusResponse(status, currentConfig, startedAtEpochMs, errorMessage,
                TransactionProcessor.PARTITION_KEY_FIELD, currentConfig.parallelism, isGenerationPaused());
    }
}
