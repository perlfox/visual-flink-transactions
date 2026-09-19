package com.example.flink.control.web;

import com.example.flink.control.model.InterrogationReport;
import com.example.flink.control.service.InterrogationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.NoSuchElementException;

/** Live Feed "Interrogate" mode: pause generation and explain exactly how one transaction was processed. */
@RestController
@RequestMapping("/api/interrogate")
public class InterrogationController {

    private final InterrogationService interrogationService;

    public InterrogationController(InterrogationService interrogationService) {
        this.interrogationService = interrogationService;
    }

    /** Pauses generation and builds the step-by-step report for one buffered transaction. */
    @PostMapping("/{transactionId}")
    public ResponseEntity<?> interrogate(@PathVariable("transactionId") long transactionId) {
        try {
            InterrogationReport report = interrogationService.interrogate(transactionId);
            return ResponseEntity.ok(report);
        } catch (NoSuchElementException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    /** Pauses generation without pinning it to any one transaction (the toolbar's "Pause" button). */
    @PostMapping("/pause")
    public ResponseEntity<?> pause() {
        try {
            interrogationService.pause();
            return ResponseEntity.ok(Map.of("paused", true));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        }
    }

    /** Resumes generation (the toolbar's "Resume Generation" button). */
    @PostMapping("/resume")
    public ResponseEntity<?> resume() {
        interrogationService.resume();
        return ResponseEntity.ok(Map.of("paused", false));
    }
}
