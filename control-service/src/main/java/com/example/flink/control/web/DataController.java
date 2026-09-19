package com.example.flink.control.web;

import com.example.flink.control.model.StatsResponse;
import com.example.flink.control.model.TransactionView;
import com.example.flink.control.service.LiveDataStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Read-only access to the current run's buffered transactions and live aggregates. */
@RestController
@RequestMapping("/api")
public class DataController {

    private final LiveDataStore liveDataStore;

    public DataController(LiveDataStore liveDataStore) {
        this.liveDataStore = liveDataStore;
    }

    /** Most recent buffered transactions, newest first; limit is capped at 5000 (the buffer's own cap). */
    @GetMapping("/transactions/recent")
    public List<TransactionView> recent(@RequestParam(name = "limit", defaultValue = "200") int limit) {
        return liveDataStore.recent(Math.min(limit, 5000));
    }

    /** Live totals (volume, counts, top accounts) for the currently (or most recently) running job. */
    @GetMapping("/stats")
    public StatsResponse stats() {
        return liveDataStore.stats();
    }
}
