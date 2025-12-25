package com.nats.pattern.requester.app.config;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.time.Duration;

@Configuration
public class NatsConfig {

    private static final Logger log = LoggerFactory.getLogger(NatsConfig.class);

    @Value("${nats.server.url}")
    private String natsServerUrl;

    @Value("${nats.connection.name}")
    private String connectionName;

    @Value("${nats.connection.max-reconnect}")
    private int maxReconnect;

    @Value("${nats.connection.reconnect-wait}")
    private long reconnectWait;

    @Value("${nats.connection.ping-interval}")
    private long pingInterval;

    private Connection natsConnection;

    @Bean
    public Connection natsConnection() throws IOException, InterruptedException {
        log.info("Connecting to NATS server at: {}", natsServerUrl);

        Options options = new Options.Builder()
                .server(natsServerUrl)
                .connectionName(connectionName)
                .connectionTimeout(Duration.ofSeconds(10))
                .pingInterval(Duration.ofMillis(pingInterval))
                .reconnectWait(Duration.ofMillis(reconnectWait))
                .maxReconnects(maxReconnect)
                .reconnectBufferSize(8 * 1024 * 1024) // 8MB
                .connectionListener((conn, type) -> {
                    log.info("NATS connection status changed: {} for connection: {}",
                            type, conn.getConnectedUrl());
                })
                .errorListener(new io.nats.client.ErrorListener() {
                    @Override
                    public void errorOccurred(Connection conn, String error) {
                        log.error("NATS error occurred: {}", error);
                    }

                    @Override
                    public void exceptionOccurred(Connection conn, Exception exp) {
                        log.error("NATS exception occurred", exp);
                    }

                    @Override
                    public void slowConsumerDetected(Connection conn,
                                                     io.nats.client.Consumer consumer) {
                        log.warn("Slow consumer detected on connection: {}",
                                conn.getConnectedUrl());
                    }
                })
                .build();

        natsConnection = Nats.connect(options);
        log.info("Successfully connected to NATS server. Connection status: {}",
                natsConnection.getStatus());
        return natsConnection;
    }

    @PreDestroy
    public void closeConnection() {
        if (natsConnection != null && natsConnection.getStatus() == Connection.Status.CONNECTED) {
            try {
                log.info("Closing NATS connection gracefully");
                natsConnection.drain(Duration.ofSeconds(5));
                natsConnection.close();
                log.info("NATS connection closed successfully");
            } catch (Exception e) {
                log.error("Error closing NATS connection", e);
            }
        }
    }
}
