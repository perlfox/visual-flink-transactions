package com.example.flink.control.model;

public class JobStatusResponse {

    public JobStatus status;
    public JobConfig config;
    public Long startedAtEpochMs;
    public String errorMessage;

    public JobStatusResponse(JobStatus status, JobConfig config, Long startedAtEpochMs, String errorMessage) {
        this.status = status;
        this.config = config;
        this.startedAtEpochMs = startedAtEpochMs;
        this.errorMessage = errorMessage;
    }
}
