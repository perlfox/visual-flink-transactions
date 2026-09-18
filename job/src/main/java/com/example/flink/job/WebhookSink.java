package com.example.flink.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class WebhookSink extends RichSinkFunction<TransactionProcessor.Transaction> {
    private transient HttpClient client;
    private transient ObjectMapper mapper;
    private final boolean enabled;
    private final String url;

    public WebhookSink(boolean enabled, String url) {
        this.enabled = enabled;
        this.url = url;
    }

    @Override
    public void open(Configuration parameters) {
        // Initialize the client once per TaskManager subtask
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(TransactionProcessor.Transaction value, Context context) {
        if (!enabled) {
            return;
        }
        try {
            String json = mapper.writeValueAsString(value);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            // Send async to avoid blocking the Flink pipeline too much
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            // Log errors but don't kill the job
            System.err.println("Failed to send to webhook: " + e.getMessage());
        }
    }
}
