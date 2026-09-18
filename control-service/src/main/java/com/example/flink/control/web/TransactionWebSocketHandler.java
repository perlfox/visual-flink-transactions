package com.example.flink.control.web;

import com.example.flink.control.model.TransactionView;
import com.example.flink.control.service.LiveDataStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broadcasts every live transaction to connected browsers as JSON text frames.
 * The source pipeline is unthrottled, so sessions are wrapped to send asynchronously
 * with bounded buffering: a slow/stuck browser gets dropped rather than blocking the
 * Flink pipeline thread that calls LiveDataStore.record(...) -> this broadcast.
 */
@Component
public class TransactionWebSocketHandler extends TextWebSocketHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionWebSocketHandler.class);
    private static final int SEND_TIME_LIMIT_MS = 5000;
    private static final int BUFFER_SIZE_LIMIT_BYTES = 1024 * 1024;

    private final ObjectMapper objectMapper;
    // Keyed by session id, since the value stored is a decorator wrapping the raw session
    // that afterConnectionClosed(...) is called with.
    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public TransactionWebSocketHandler(LiveDataStore liveDataStore, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        liveDataStore.subscribe(this::broadcast);
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        sessions.put(session.getId(), new ConcurrentWebSocketSessionDecorator(session, SEND_TIME_LIMIT_MS, BUFFER_SIZE_LIMIT_BYTES));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
    }

    private void broadcast(TransactionView view) {
        if (sessions.isEmpty()) {
            return;
        }
        try {
            TextMessage message = new TextMessage(objectMapper.writeValueAsString(view));
            for (WebSocketSession session : sessions.values()) {
                try {
                    if (session.isOpen()) {
                        session.sendMessage(message);
                    }
                } catch (IOException e) {
                    LOG.debug("Dropping message for closed/broken session {}", session.getId());
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to serialize transaction for broadcast", e);
        }
    }
}
