package com.example.flink.control.config;

import com.example.flink.control.web.TransactionWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final TransactionWebSocketHandler transactionWebSocketHandler;

    public WebSocketConfig(TransactionWebSocketHandler transactionWebSocketHandler) {
        this.transactionWebSocketHandler = transactionWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(transactionWebSocketHandler, "/ws/transactions")
                .setAllowedOrigins("*");
    }
}
