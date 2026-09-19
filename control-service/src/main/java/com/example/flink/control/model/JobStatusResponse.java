package com.example.flink.control.model;

/** GET /api/job/status and POST /api/job/* response body: current lifecycle state plus enough context to render it. */
public class JobStatusResponse {

    public JobStatus status;
    public JobConfig config;
    public Long startedAtEpochMs;

    /** Raw {@code Exception.getMessage()} from the last failed start/stop, surfaced to the UI as-is; null otherwise. */
    public String errorMessage;
    public String partitionKey;
    public int parallelism;

    /** Whether the "Interrogate" pause switch ({@link com.example.flink.job.TransactionSourceControl}) is currently on. */
    public boolean generationPaused;

    public JobStatusResponse(JobStatus status, JobConfig config, Long startedAtEpochMs, String errorMessage,
                              String partitionKey, int parallelism, boolean generationPaused) {
        this.status = status;
        this.config = config;
        this.startedAtEpochMs = startedAtEpochMs;
        this.errorMessage = errorMessage;
        this.partitionKey = partitionKey;
        this.parallelism = parallelism;
        this.generationPaused = generationPaused;
    }
}
