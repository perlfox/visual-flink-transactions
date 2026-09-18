package com.example.flink.control.web;

import com.example.flink.control.model.JobConfig;
import com.example.flink.control.model.JobStatusResponse;
import com.example.flink.control.service.FlinkJobManager;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/job")
public class JobController {

    private final FlinkJobManager jobManager;

    public JobController(FlinkJobManager jobManager) {
        this.jobManager = jobManager;
    }

    @GetMapping("/status")
    public JobStatusResponse status() {
        return jobManager.statusResponse();
    }

    @PostMapping("/start")
    public ResponseEntity<?> start(@RequestBody(required = false) JobConfig config) {
        try {
            jobManager.start(config != null ? config : new JobConfig());
            return ResponseEntity.accepted().body(jobManager.statusResponse());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stop() {
        try {
            jobManager.stop();
            return ResponseEntity.accepted().body(jobManager.statusResponse());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }
}
