package com.example.flink.control.web;

import com.example.flink.control.model.JobConfig;
import com.example.flink.control.model.JobStatusResponse;
import com.example.flink.control.service.FlinkJobManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Lifecycle controls for the embedded Flink job (start/stop/clear-data) plus config persistence.
 * All endpoints delegate straight to {@link FlinkJobManager}; an {@link IllegalStateException}
 * from a bad lifecycle transition (e.g. starting an already-running job) is reported as 409
 * Conflict rather than a generic 500.
 */
@RestController
@RequestMapping("/api/job")
public class JobController {

    private final FlinkJobManager jobManager;

    public JobController(FlinkJobManager jobManager) {
        this.jobManager = jobManager;
    }

    /** Current lifecycle status, effective config, and generation-paused flag; polled by the UI. */
    @GetMapping("/status")
    public JobStatusResponse status() {
        return jobManager.statusResponse();
    }

    /** Starts a fresh job run with the given config (or defaults if none supplied), clearing prior data. */
    @PostMapping("/start")
    public ResponseEntity<?> start(@RequestBody(required = false) JobConfig config) {
        try {
            jobManager.start(config != null ? config : new JobConfig());
            return ResponseEntity.accepted().body(jobManager.statusResponse());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    /** Persists config for the next Start without starting a job; rejected while one is already running. */
    @PostMapping("/config")
    public ResponseEntity<?> updateConfig(@RequestBody(required = false) JobConfig config) {
        try {
            return ResponseEntity.ok(jobManager.updateConfig(config != null ? config : new JobConfig()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    /** Cancels the running job; a no-op error (409) if no job is currently running. */
    @PostMapping("/stop")
    public ResponseEntity<?> stop() {
        try {
            jobManager.stop();
            return ResponseEntity.accepted().body(jobManager.statusResponse());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    /** Resets all accumulated data; restarts the job with its current config if one is running. */
    @PostMapping("/clear-data")
    public ResponseEntity<?> clearData() {
        try {
            jobManager.clearData();
            return ResponseEntity.accepted().body(jobManager.statusResponse());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }
}
