package com.nats.pattern.requester.app.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class NatsRequesterService {

    private static final Logger log = LoggerFactory.getLogger(NatsRequesterService.class);

    private final Connection natsConnection;

    @Value("${nats.request.timeout}")
    private long requestTimeout;

    public NatsRequesterService(Connection natsConnection) {
        this.natsConnection = natsConnection;
    }

    /**
     * Synchronous request-reply
     * Uses Java 21 Virtual Threads for non-blocking I/O
     */
    public String sendSyncRequest(String subject, String payload) {
        try {
            log.debug("Sending synchronous request to subject: {} with payload: {}",
                    subject, payload);

            Message reply = natsConnection.request(
                    subject,
                    payload.getBytes(StandardCharsets.UTF_8),
                    Duration.ofMillis(requestTimeout)
            );

            if (reply != null && reply.getData() != null) {
                String response = new String(reply.getData(), StandardCharsets.UTF_8);
                log.debug("Received reply: {}", response);
                return response;
            } else {
                log.warn("No response received within timeout for subject: {}", subject);
                return "No response received";
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Request interrupted for subject: {}", subject, e);
            return "Request interrupted";
        } catch (Exception e) {
            log.error("Error sending request to subject: {}", subject, e);
            return "Error: " + e.getMessage();
        }
    }

    /**
     * Asynchronous request-reply using CompletableFuture
     * Leverages Java 21 Virtual Threads for scalability
     */
    public CompletableFuture sendAsyncRequest(String subject, String payload) {
        log.debug("Sending asynchronous request to subject: {} with payload: {}",
                subject, payload);

        CompletableFuture futureMessage = natsConnection.request(
                subject,
                payload.getBytes(StandardCharsets.UTF_8)
        );

        return futureMessage
                .orTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .thenApply(msg -> {
                    if (msg != null) {
                        String response = msg.toString();//new String(msg.getData(), StandardCharsets.UTF_8);
                        log.debug("Received async reply: {}", response);
                        return response;
                    } else {
                        log.warn("Received null or empty message for subject: {}", subject);
                        return "Empty response";
                    }
                })
                .exceptionally(throwable -> {
                    Exception cause = (Exception) throwable;
                    if (cause instanceof TimeoutException) {
                        log.warn("Async request timed out for subject: {}", subject);
                        return "Request timed out";
                    }
                    log.error("Error in async request for subject: {}", subject, cause);
                    String errorMessage = cause.getMessage() != null ? cause.getMessage() : "Unknown error";
                    return "Error: " + errorMessage;
                });
    }

    /**
     * Send request with custom timeout
     */
    public String sendRequestWithTimeout(String subject, String payload, Duration timeout) {
        try {
            log.debug("Sending request with custom timeout: {} ms to subject: {}",
                    timeout.toMillis(), subject);

            Message reply = natsConnection.request(
                    subject,
                    payload.getBytes(StandardCharsets.UTF_8),
                    timeout
            );

            return reply != null && reply.getData() != null
                    ? new String(reply.getData(), StandardCharsets.UTF_8)
                    : "No response";

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Request with timeout interrupted", e);
            return "Request interrupted";
        } catch (Exception e) {
            log.error("Error sending request with timeout", e);
            return "Error: " + e.getMessage();
        }
    }

    /**
     * Send multiple parallel requests
     * Takes advantage of Java 21 Virtual Threads
     */
    public CompletableFuture sendParallelRequests(String[] subjects, String payload) {
        log.debug("Sending parallel requests to {} subjects", subjects.length);

        @SuppressWarnings("unchecked")
        CompletableFuture[] futures = new CompletableFuture[subjects.length];
        for (int i = 0; i < subjects.length; i++) {
            futures[i] = sendAsyncRequest(subjects[i], payload);
        }

        return CompletableFuture.allOf(futures)
                .thenApply(v -> {
                    StringBuilder results = new StringBuilder("Parallel Results:\n");
                    for (int i = 0; i < subjects.length; i++) {
                        try {
                            results.append(subjects[i])
                                    .append(": ")
                                    .append(futures[i].join())
                                    .append("\n");
                        } catch (Exception e) {
                            results.append(subjects[i])
                                    .append(": Error - ")
                                    .append(e.getMessage())
                                    .append("\n");
                        }
                    }
                    return results.toString();
                })
                .exceptionally(throwable -> {
                    log.error("Error in parallel requests", throwable);
                    String errorMessage = throwable.getMessage() != null ? throwable.getMessage() : "Unknown error";
                    return "Error in parallel requests: " + errorMessage;
                });
    }

    /**
     * Get connection statistics
     */
    public String getConnectionStats() {
        var stats = natsConnection.getStatistics();
        return String.format(
                "Connection Stats - In Msgs: %d, Out Msgs: %d, In Bytes: %d, Out Bytes: %d, Reconnects: %d",
                stats.getInMsgs(), stats.getOutMsgs(), stats.getInBytes(),
                stats.getOutBytes(), stats.getReconnects()
        );
    }
}