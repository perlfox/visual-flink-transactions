package com.example.flink.control.web;

import com.example.flink.control.model.StatsResponse;
import com.example.flink.control.model.TransactionView;
import com.example.flink.control.service.LiveDataStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class DataController {

    private final LiveDataStore liveDataStore;

    public DataController(LiveDataStore liveDataStore) {
        this.liveDataStore = liveDataStore;
    }

    @GetMapping("/transactions/recent")
    public List<TransactionView> recent(@RequestParam(defaultValue = "200") int limit) {
        return liveDataStore.recent(Math.min(limit, 5000));
    }

    @GetMapping("/stats")
    public StatsResponse stats() {
        return liveDataStore.stats();
    }
}
