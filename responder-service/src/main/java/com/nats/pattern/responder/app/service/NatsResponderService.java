package com.nats.pattern.responder.app.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class NatsResponderService {

    private static final Logger log = LoggerFactory.getLogger(NatsResponderService.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final Connection natsConnection;
    private Dispatcher dispatcher;
    private final AtomicLong messageCounter = new AtomicLong(0);

    @Value("#{'${nats.subjects}'.split(',')}")
    private List<String> subjects;

    public NatsResponderService(Connection natsConnection) {
        this.natsConnection = natsConnection;
    }

    @PostConstruct
    public void initialize() {
        log.info("Initializing NATS responder service with Java 21 Virtual Threads");

        // Create dispatcher for async message handling
        // With Java 21 Virtual Threads, each handler runs efficiently
        dispatcher = natsConnection.createDispatcher(msg -> {
            log.warn("Received message on default handler: {}", msg.getSubject());
        });

        // Subscribe to multiple subjects
        subjects.forEach(subject -> {
            dispatcher.subscribe((String) subject, this::handleRequest);
            log.info("Subscribed to subject: {}", subject);
        });

        log.info("NATS responder service initialized successfully. Subscribed to {} subjects",
                subjects.size());
    }

    /**
     * Handle incoming requests and send replies
     * Each request is processed on a virtual thread (Java 21)
     */
    private void handleRequest(Message message) {
        long msgNum = messageCounter.incrementAndGet();
        String subject = message.getSubject();
        String payload = message.getData() != null
                ? new String(message.getData(), StandardCharsets.UTF_8)
                : "";
        String replyTo = message.getReplyTo();

        log.debug("Received request #{} on subject: {} with payload: {}",
                msgNum, subject, payload);

        if (replyTo == null || replyTo.isEmpty()) {
            log.warn("No reply-to subject provided for message #{}, cannot send response", msgNum);
            return;
        }

        try {
            // Process the request based on subject
            String response = processRequest(subject, payload, msgNum);

            // Send the reply
            natsConnection.publish(replyTo, response.getBytes(StandardCharsets.UTF_8));
            log.debug("Sent reply #{}: {}", msgNum, response);

        } catch (Exception e) {
            log.error("Error processing request #{}", msgNum, e);
            String errorResponse = String.format(
                    "Error processing request: %s at %s",
                    e.getMessage(),
                    LocalDateTime.now().format(FORMATTER)
            );
            try {
                natsConnection.publish(
                        replyTo,
                        errorResponse.getBytes(StandardCharsets.UTF_8)
                );
            } catch (Exception ex) {
                log.error("Error sending error response for message #{}", msgNum, ex);
            }
        }
    }

    /**
     * Business logic to process different types of requests
     */
    private String processRequest(String subject, String payload, long msgNum) {
        log.info("Processing request #{} for subject: {}", msgNum, subject);

        return switch (subject) {
            case "order.process" -> processOrder(payload, msgNum);
            case "user.validate" -> validateUser(payload, msgNum);
            case "payment.authorize" -> authorizePayment(payload, msgNum);
            case "inventory.check" -> checkInventory(payload, msgNum);
            case "notification.send" -> sendNotification(payload, msgNum);
            default -> String.format("Unknown subject: %s at %s",
                    subject,
                    LocalDateTime.now().format(FORMATTER));
        };
    }

    private String processOrder(String orderData, long msgNum) {
        log.info("Processing order #{}: {}", msgNum, orderData);
        simulateProcessing(100);

        return String.format(
                """
                {
                  "status": "success",
                  "message": "Order processed successfully",
                  "orderId": "ORD-%d",
                  "orderData": "%s",
                  "timestamp": "%s",
                  "processingTime": "100ms"
                }
                """,
                msgNum,
                orderData,
                LocalDateTime.now().format(FORMATTER)
        );
    }

    private String validateUser(String userData, long msgNum) {
        log.info("Validating user #{}: {}", msgNum, userData);
        simulateProcessing(50);

        boolean isValid = userData != null && !userData.trim().isEmpty()
                && userData.length() >= 3;

        return String.format(
                """
                {
                  "status": "%s",
                  "message": "User validation %s",
                  "userData": "%s",
                  "valid": %b,
                  "timestamp": "%s"
                }
                """,
                isValid ? "success" : "failed",
                isValid ? "passed" : "failed",
                userData,
                isValid,
                LocalDateTime.now().format(FORMATTER)
        );
    }

    private String authorizePayment(String paymentData, long msgNum) {
        log.info("Authorizing payment #{}: {}", msgNum, paymentData);
        simulateProcessing(200);

        return String.format(
                """
                {
                  "status": "success",
                  "message": "Payment authorized",
                  "paymentId": "PAY-%d",
                  "reference": "PAY-%d-%d",
                  "paymentData": "%s",
                  "timestamp": "%s"
                }
                """,
                msgNum,
                msgNum,
                System.currentTimeMillis(),
                paymentData,
                LocalDateTime.now().format(FORMATTER)
        );
    }

    private String checkInventory(String itemData, long msgNum) {
        log.info("Checking inventory #{}: {}", msgNum, itemData);
        simulateProcessing(75);

        int availableStock = (int) (Math.random() * 100);

        return String.format(
                """
                {
                  "status": "success",
                  "message": "Inventory check completed",
                  "itemData": "%s",
                  "availableStock": %d,
                  "inStock": %b,
                  "timestamp": "%s"
                }
                """,
                itemData,
                availableStock,
                availableStock > 0,
                LocalDateTime.now().format(FORMATTER)
        );
    }

    private String sendNotification(String notificationData, long msgNum) {
        log.info("Sending notification #{}: {}", msgNum, notificationData);
        simulateProcessing(150);

        return String.format(
                """
                {
                  "status": "success",
                  "message": "Notification sent",
                  "notificationId": "NOTIF-%d",
                  "notificationData": "%s",
                  "timestamp": "%s"
                }
                """,
                msgNum,
                notificationData,
                LocalDateTime.now().format(FORMATTER)
        );
    }

    private void simulateProcessing(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Processing simulation interrupted");
        }
    }

    @PreDestroy
    public void cleanup() {
        log.info("Cleaning up NATS responder service");
        if (dispatcher != null) {
            subjects.forEach(subject -> {
                try {
                    dispatcher.unsubscribe((String) subject);
                    log.info("Unsubscribed from subject: {}", subject);
                } catch (Exception e) {
                    log.error("Error unsubscribing from subject: {}", subject, e);
                }
            });
        }
        log.info("Total messages processed: {}", messageCounter.get());
    }
}
