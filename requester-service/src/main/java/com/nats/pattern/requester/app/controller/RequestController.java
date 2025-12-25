package com.nats.pattern.requester.app.controller;


import com.nats.pattern.requester.app.service.NatsRequesterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/request")
public class RequestController {

    private static final Logger log = LoggerFactory.getLogger(RequestController.class);

    private final NatsRequesterService requesterService;

    public RequestController(NatsRequesterService requesterService) {
        this.requesterService = requesterService;
    }

    @PostMapping("/sync/{subject}")
    public ResponseEntity<Map> sendSyncRequest(
            @PathVariable String subject,
            @RequestBody String payload) {

        log.info("Received sync request for subject: {}", subject);
        long startTime = System.currentTimeMillis();

        String response = requesterService.sendSyncRequest(subject, payload);
        long duration = System.currentTimeMillis() - startTime;

        return ResponseEntity.ok(Map.of(
                "subject", subject,
                "request", payload,
                "response", response,
                "duration_ms", String.valueOf(duration)
        ));
    }

    @PostMapping("/async/{subject}")
    public CompletableFuture<ResponseEntity<Map>> sendAsyncRequest(
            @PathVariable String subject,
            @RequestBody String payload) {

        log.info("Received async request for subject: {}", subject);
        long startTime = System.currentTimeMillis();

        return requesterService.sendAsyncRequest(subject, payload)
                .thenApply(response -> {
                    long duration = System.currentTimeMillis() - startTime;
                    return ResponseEntity.ok(Map.of(
                            "subject", subject,
                            "request", payload,
                            "response", response,
                            "duration_ms", String.valueOf(duration)
                    ));
                });
    }

    @PostMapping("/custom-timeout/{subject}")
    public ResponseEntity<Map> sendRequestWithCustomTimeout(
            @PathVariable String subject,
            @RequestParam(defaultValue = "3000") long timeoutMs,
            @RequestBody String payload) {

        log.info("Received request with custom timeout: {} ms", timeoutMs);
        long startTime = System.currentTimeMillis();

        String response = requesterService.sendRequestWithTimeout(
                subject,
                payload,
                Duration.ofMillis(timeoutMs)
        );
        long duration = System.currentTimeMillis() - startTime;

        return ResponseEntity.ok(Map.of(
                "subject", subject,
                "request", payload,
                "response", response,
                "timeout_ms", String.valueOf(timeoutMs),
                "duration_ms", String.valueOf(duration)
        ));
    }

    @PostMapping("/parallel")
    public CompletableFuture<ResponseEntity<Map>> sendParallelRequests(
            @RequestParam String[] subjects,
            @RequestBody String payload) {

        log.info("Received parallel request for {} subjects", subjects.length);

        return requesterService.sendParallelRequests(subjects, payload)
                .thenApply(response -> ResponseEntity.ok(Map.of(
                        "subjects", String.join(", ", subjects),
                        "request", payload,
                        "response", response
                )));
    }

    @GetMapping("/stats")
    public ResponseEntity getStats() {
        return ResponseEntity.ok(requesterService.getConnectionStats());
    }
}
