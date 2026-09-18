package com.example.flink.control.service;

import com.example.flink.control.model.JobConfig;
import com.example.flink.control.model.JobStatus;
import com.example.flink.control.model.JobStatusResponse;
import com.example.flink.job.TransactionEventBus;
import com.example.flink.job.TransactionProcessor;
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
    private static final int PARALLELISM = 2;

    private final LiveDataStore liveDataStore;
    private final TransactionEventBus.TransactionListener eventListener;
    private final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "flink-job-lifecycle");
        t.setDaemon(true);
        return t;
    });

    private volatile JobStatus status = JobStatus.STOPPED;
    private volatile JobConfig currentConfig;
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
        status = JobStatus.STARTING;
        errorMessage = null;
        currentConfig = config;
        liveDataStore.reset();

        lifecycleExecutor.submit(() -> {
            try {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARALLELISM);
                env.enableCheckpointing(config.checkpointIntervalMs);
                env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

                TransactionEventBus.subscribe(eventListener);
                TransactionProcessor.buildPipeline(env, config.maxAccounts, config.stateBloat,
                        config.webhookEnabled, config.webhookUrl);

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
        });
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

    public JobStatusResponse statusResponse() {
        return new JobStatusResponse(status, currentConfig, startedAtEpochMs, errorMessage);
    }
}
