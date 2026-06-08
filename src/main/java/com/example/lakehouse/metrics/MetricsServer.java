package com.example.lakehouse.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Lightweight HTTP server that exposes Prometheus metrics at {@code /metrics}
 * using the JDK's built-in {@link HttpServer}.
 *
 * @author Alexander Castro
 */
public class MetricsServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MetricsServer.class);

    /** The Prometheus registry whose scraped output is served at /metrics. */
    private final PrometheusMeterRegistry registry;

    /** The TCP port the server listens on. */
    private final int port;

    /** The underlying JDK HTTP server; null until {@link #start()} is called. */
    private HttpServer server;

    /**
     * Constructs a MetricsServer that will serve the given registry on the given port.
     *
     * @param registry the Prometheus registry to expose; must not be null
     * @param port     the TCP port to listen on; must be in the range [1024, 65535]
     */
    public MetricsServer(PrometheusMeterRegistry registry, int port) {
        this.registry = registry;
        this.port = port;
    }

    /**
     * Binds to the configured port and begins serving requests at {@code /metrics}.
     * Must be called exactly once before metrics are accessible.
     *
     * @throws IOException if the server socket cannot be bound to the port
     */
    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        log.info("metrics_server_started port={}", port);
    }

    /**
     * Stops the HTTP server immediately. Safe to call if {@link #start()} was
     * never invoked.
     */
    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
            log.info("metrics_server_stopped port={}", port);
        }
    }
}
