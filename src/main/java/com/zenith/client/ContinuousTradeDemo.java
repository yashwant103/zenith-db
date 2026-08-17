package com.zenith.client;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * ZenithDB ContinuousTradeDemo
 *
 * Interview/demo workload:
 *   - 60,000 unique INSERT trades
 *   - runs for approximately 150 seconds (2.5 minutes)
 *   - 20 concurrent workers
 *   - ~400 client operations/sec target
 *   - automatically discovers/re-discovers the current Raft leader
 *   - keeps sending traffic continuously so Grafana has a long visible window
 *
 * Usage:
 *   mvn clean compile exec:java -Dexec.mainClass="com.zenith.client.ContinuousTradeDemo"
 *
 * Optional arguments:
 *   args[0] = trade count (default 60000)
 *   args[1] = duration seconds (default 150)
 *   args[2] = worker count (default 20)
 *
 * Example:
 *   mvn exec:java -Dexec.mainClass="com.zenith.client.ContinuousTradeDemo" \
 *       -Dexec.args="60000 150 20"
 */
public final class ContinuousTradeDemo {

    private static final String HOST = "localhost";

    private static final int[] NODE_PORTS = {9001, 9002, 9003};

    private static final int DEFAULT_TRADES = 60_000;
    private static final int DEFAULT_DURATION_SECONDS = 150;
    private static final int DEFAULT_WORKERS = 20;

    private static final int CONNECT_TIMEOUT_MS = 1_500;
    private static final int READ_TIMEOUT_MS = 3_000;
    private static final int RETRY_BACKOFF_MS = 50;

    private static final List<String> TICKERS = List.of(
            "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
            "NVDA", "META", "JPM", "BAC", "GS",
            "MS", "BLK"
    );

    private static final AtomicInteger leaderPort = new AtomicInteger(9001);
    private static final AtomicInteger nextTrade = new AtomicInteger(1);

    private static final LongAdder submitted = new LongAdder();
    private static final LongAdder successes = new LongAdder();
    private static final LongAdder failures = new LongAdder();
    private static final LongAdder leaderRedirects = new LongAdder();
    private static final LongAdder retries = new LongAdder();

    private static final AtomicBoolean running = new AtomicBoolean(true);

    private ContinuousTradeDemo() {}

    public static void main(String[] args) throws Exception {

        int totalTrades = parseInt(args, 0, DEFAULT_TRADES);
        int durationSeconds = parseInt(args, 1, DEFAULT_DURATION_SECONDS);
        int workers = parseInt(args, 2, DEFAULT_WORKERS);

        if (totalTrades <= 0 || durationSeconds <= 0 || workers <= 0) {
            throw new IllegalArgumentException(
                    "trade count, duration and workers must be > 0"
            );
        }

        double targetOpsPerSecond =
                (double) totalTrades / durationSeconds;

        printBanner(
                totalTrades,
                durationSeconds,
                workers,
                targetOpsPerSecond
        );

        System.out.println("Finding current Raft leader...");

        int discovered = discoverLeader();

        if (discovered == -1) {
            System.err.println(
                    "No leader found on ports 9001/9002/9003."
            );
            System.err.println(
                    "Run: docker compose up -d --build"
            );
            return;
        }

        leaderPort.set(discovered);

        System.out.println(
                "✓ Leader found on port " + discovered
        );

        System.out.println();
        System.out.println("Grafana     : http://localhost:3000");
        System.out.println("Prometheus  : http://localhost:9090");
        System.out.println();

        System.out.println("Starting in 3 seconds...");
        Thread.sleep(3000);

        Instant start = Instant.now();

        Instant deadline =
                start.plusSeconds(durationSeconds);

        ScheduledExecutorService progress =
                Executors.newSingleThreadScheduledExecutor();

        ExecutorService pool =
                Executors.newFixedThreadPool(workers);

        progress.scheduleAtFixedRate(
                () -> printProgress(
                        start,
                        totalTrades,
                        deadline
                ),
                5,
                5,
                TimeUnit.SECONDS
        );

        List<Future<?>> futures = new ArrayList<>();

        try {

            for (int i = 0; i < workers; i++) {

                final int workerId = i;

                futures.add(
                        pool.submit(() ->
                                workerLoop(
                                        workerId,
                                        totalTrades,
                                        deadline,
                                        targetOpsPerSecond,
                                        workers
                                )
                        )
                );
            }

            for (Future<?> future : futures) {

                try {
                    future.get();

                } catch (ExecutionException e) {

                    System.err.println(
                            "Worker failed: " +
                                    e.getCause()
                    );
                }
            }

        } finally {

            running.set(false);

            progress.shutdownNow();

            pool.shutdownNow();
        }

        long elapsedMs =
                Duration.between(
                        start,
                        Instant.now()
                ).toMillis();

        printFinal(totalTrades, elapsedMs);
    }

    private static void workerLoop(
            int workerId,
            int totalTrades,
            Instant deadline,
            double targetOpsPerSecond,
            int workers) {

        double workerRate =
                targetOpsPerSecond / workers;

        long nanosPerOperation =
                workerRate <= 0
                        ? 0
                        : (long)
                        (1_000_000_000.0 / workerRate);

        Socket socket = null;

        BufferedReader in = null;

        BufferedWriter out = null;

        try {

            while (
                    running.get()
                            && Instant.now().isBefore(deadline)
                            && nextTrade.get() <= totalTrades
            ) {

                int tradeNumber =
                        nextTrade.getAndIncrement();

                if (tradeNumber > totalTrades) {
                    break;
                }

                String tradeId =
                        String.format(
                                "DEMO%06d",
                                tradeNumber
                        );

                String ticker =
                        TICKERS.get(
                                (tradeNumber - 1)
                                        % TICKERS.size()
                        );

                int quantity =
                        100 + (tradeNumber % 901);

                double price =
                        100.00 +
                                ((tradeNumber * 37) % 10000)
                                        / 100.0;

                String requestId =
                        "DEMOREQ-" + tradeId;

                String command =
                        String.format(
                                Locale.US,
                                "INSERT,%s,%s,%d,%.2f,PENDING,%s",
                                tradeId,
                                ticker,
                                quantity,
                                price,
                                requestId
                        );

                long opStart =
                        System.nanoTime();

                try {

                    if (
                            socket == null
                                    || socket.isClosed()
                    ) {

                        Connection connection =
                                connectToLeader();

                        socket = connection.socket();

                        in = connection.in();

                        out = connection.out();
                    }

                    submitted.increment();

                    String response =
                            send(
                                    socket,
                                    in,
                                    out,
                                    command
                            );

                    if (isSuccess(response)) {

                        successes.increment();

                    } else if (
                            isFollowerResponse(response)
                    ) {

                        leaderRedirects.increment();

                        closeQuietly(socket);

                        socket = null;

                        retries.increment();

                        retryTrade(command);

                    } else {

                        failures.increment();
                    }

                } catch (SocketTimeoutException e) {

                    failures.increment();

                    retries.increment();

                    closeQuietly(socket);

                    socket = null;

                } catch (IOException e) {

                    failures.increment();

                    retries.increment();

                    closeQuietly(socket);

                    socket = null;

                    sleepQuietly(
                            RETRY_BACKOFF_MS
                    );

                } finally {

                    long spent =
                            System.nanoTime()
                                    - opStart;

                    long remaining =
                            nanosPerOperation
                                    - spent;

                    if (remaining > 0) {

                        LockSupport.parkNanos(
                                remaining
                        );
                    }
                }
            }

        } finally {

            closeQuietly(socket);
        }
    }

    private static void retryTrade(
            String command) {

        for (
                int attempt = 0;
                attempt < 2 && running.get();
                attempt++
        ) {

            Socket socket = null;

            try {

                Connection connection =
                        connectToLeader();

                socket =
                        connection.socket();

                String response =
                        send(
                                socket,
                                connection.in(),
                                connection.out(),
                                command
                        );

                if (isSuccess(response)) {

                    successes.increment();

                    return;
                }

                if (
                        !isFollowerResponse(response)
                ) {

                    return;
                }

                leaderRedirects.increment();

            } catch (IOException ignored) {

                // Caller already records the failure.

            } finally {

                closeQuietly(socket);
            }

            sleepQuietly(
                    RETRY_BACKOFF_MS
            );
        }
        failures.increment();
    }

    private static Connection connectToLeader()
            throws IOException {

        int preferred =
                leaderPort.get();

        int[] candidates =
                candidateOrder(preferred);

        IOException last = null;

        for (int port : candidates) {

            try {

                Socket socket =
                        new Socket();

                socket.connect(
                        new InetSocketAddress(
                                HOST,
                                port
                        ),
                        CONNECT_TIMEOUT_MS
                );

                socket.setSoTimeout(
                        READ_TIMEOUT_MS
                );

                BufferedReader in =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()
                                )
                        );

                BufferedWriter out =
                        new BufferedWriter(
                                new OutputStreamWriter(
                                        socket.getOutputStream()
                                )
                        );

                String probeId =
                        "LEADER_PROBE_" +
                                Thread.currentThread()
                                        .getId();

                String response =
                        send(
                                socket,
                                in,
                                out,
                                "SELECT," + probeId
                        );

                if (isFollowerResponse(response)) {

                    closeQuietly(socket);

                    continue;
                }

                leaderPort.set(port);

                return new Connection(
                        socket,
                        in,
                        out
                );

            } catch (IOException e) {

                last = e;
            }
        }

        throw new IOException(
                "Could not connect to a current leader",
                last
        );
    }

    private static int discoverLeader() {

        for (int port : NODE_PORTS) {

            Socket socket = null;

            try {

                socket =
                        new Socket();

                socket.connect(
                        new InetSocketAddress(
                                HOST,
                                port
                        ),
                        CONNECT_TIMEOUT_MS
                );

                socket.setSoTimeout(
                        READ_TIMEOUT_MS
                );

                BufferedReader in =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()
                                )
                        );

                BufferedWriter out =
                        new BufferedWriter(
                                new OutputStreamWriter(
                                        socket.getOutputStream()
                                )
                        );

                String response =
                        send(
                                socket,
                                in,
                                out,
                                "SELECT,LEADER_DISCOVERY_"
                                        + port
                        );

                if (!isFollowerResponse(response)) {

                    return port;
                }

            } catch (IOException ignored) {

                // Try next node.

            } finally {

                closeQuietly(socket);
            }
        }

        return -1;
    }

    private static String send(
            Socket socket,
            BufferedReader in,
            BufferedWriter out,
            String command)
            throws IOException {

        out.write(command);

        out.write("\n");

        out.flush();

        String response =
                in.readLine();

        if (response == null) {

            throw new EOFException(
                    "Server closed the connection"
            );
        }

        return response;
    }

    private static boolean isSuccess(
            String response) {

        if (response == null) {
            return false;
        }

        String r =
                response.toUpperCase(
                        Locale.ROOT
                );

        return r.startsWith("PENDING:")
                || r.startsWith("SUCCESS:")
                || r.startsWith("OK:")
                || r.startsWith("INSERTED:")
                || r.contains("SUBMITTED");
    }

    private static boolean isFollowerResponse(
            String response) {

        if (response == null) {
            return false;
        }

        String r =
                response.toUpperCase(
                        Locale.ROOT
                );

        return r.contains("FOLLOWER")
                || r.contains("NOT THE LEADER")
                || r.contains("REDIRECT")
                || r.contains("CANNOT GUARANTEE");   // ADD THIS LINE
    }

    private static int[] candidateOrder(
            int preferred) {

        int[] result =
                new int[NODE_PORTS.length];

        int index = 0;

        result[index++] =
                preferred;

        for (int port : NODE_PORTS) {

            if (port != preferred) {

                result[index++] =
                        port;
            }
        }

        return result;
    }

    private static void printProgress(
            Instant start,
            int totalTrades,
            Instant deadline) {

        long elapsedMs =
                Duration.between(
                        start,
                        Instant.now()
                ).toMillis();

        double elapsedSec =
                Math.max(
                        elapsedMs / 1000.0,
                        0.001
                );

        long done =
                successes.sum()
                        + failures.sum();

        System.out.printf(
                Locale.US,
                "  %6.1f ops/sec | submitted: %,d | success: %,d | failures: %,d | leader: %d | remaining: %,d%n",
                done / elapsedSec,
                submitted.sum(),
                successes.sum(),
                failures.sum(),
                leaderPort.get(),
                Math.max(
                        0,
                        totalTrades
                                - nextTrade.get()
                                + 1
                )
        );
    }

    private static void printFinal(
            int totalTrades,
            long elapsedMs) {

        double seconds =
                Math.max(
                        elapsedMs / 1000.0,
                        0.001
                );

        long completed =
                successes.sum()
                        + failures.sum();

        System.out.println();

        System.out.println(
                "╔══════════════════════════════════════════════════════════╗"
        );

        System.out.println(
                "║          ZenithDB CONTINUOUS TRADE DEMO                 ║"
        );

        System.out.println(
                "╠══════════════════════════════════════════════════════════╣"
        );

        System.out.printf(
                Locale.US,
                "║ Target trades       : %,d%n",
                totalTrades
        );

        System.out.printf(
                Locale.US,
                "║ Duration             : %.2f sec%n",
                seconds
        );

        System.out.printf(
                Locale.US,
                "║ Operations attempted : %,d%n",
                completed
        );

        System.out.printf(
                Locale.US,
                "║ Successful           : %,d%n",
                successes.sum()
        );

        System.out.printf(
                Locale.US,
                "║ Failed               : %,d%n",
                failures.sum()
        );

        System.out.printf(
                Locale.US,
                "║ Throughput           : %.2f ops/sec%n",
                completed / seconds
        );

        System.out.printf(
                Locale.US,
                "║ Success throughput   : %.2f ops/sec%n",
                successes.sum() / seconds
        );

        System.out.printf(
                Locale.US,
                "║ Leader redirects     : %,d%n",
                leaderRedirects.sum()
        );

        System.out.printf(
                Locale.US,
                "║ Retries              : %,d%n",
                retries.sum()
        );

        System.out.println(
                "╠══════════════════════════════════════════════════════════╣"
        );

        System.out.println(
                "║ Grafana              : http://localhost:3000            ║"
        );

        System.out.println(
                "║ Prometheus           : http://localhost:9090            ║"
        );

        System.out.println(
                "╚══════════════════════════════════════════════════════════╝"
        );

        System.out.println();

        System.out.println(
                "Demo complete. Keep Grafana open to show the 2–3 minute workload."
        );
    }

    private static void printBanner(
            int totalTrades,
            int durationSeconds,
            int workers,
            double targetOpsPerSecond) {

        System.out.println(
                "╔══════════════════════════════════════════════════════════╗"
        );

        System.out.println(
                "║       ZenithDB Continuous Trade Demo                    ║"
        );

        System.out.println(
                "║       Interview-ready 2–3 minute workload               ║"
        );

        System.out.println(
                "╠══════════════════════════════════════════════════════════╣"
        );

        System.out.printf(
                Locale.US,
                "║ Trades               : %,d%n",
                totalTrades
        );

        System.out.printf(
                Locale.US,
                "║ Duration             : %d sec (%.1f min)%n",
                durationSeconds,
                durationSeconds / 60.0
        );

        System.out.printf(
                Locale.US,
                "║ Workers              : %d%n",
                workers
        );

        System.out.printf(
                Locale.US,
                "║ Target client rate   : %.1f ops/sec%n",
                targetOpsPerSecond
        );

        System.out.println(
                "║ Workload             : 100% unique INSERT trades       ║"
        );

        System.out.println(
                "║ Leader discovery     : automatic                        ║"
        );

        System.out.println(
                "║ Idempotency          : unique requestId per trade       ║"
        );

        System.out.println(
                "╚══════════════════════════════════════════════════════════╝"
        );
    }

    private static int parseInt(
            String[] args,
            int index,
            int defaultValue) {

        if (args.length <= index) {
            return defaultValue;
        }

        try {

            return Integer.parseInt(
                    args[index]
            );

        } catch (NumberFormatException e) {

            return defaultValue;
        }
    }

    private static void sleepQuietly(
            long millis) {

        try {

            Thread.sleep(millis);

        } catch (InterruptedException e) {

            Thread.currentThread()
                    .interrupt();
        }
    }

    private static void closeQuietly(
            Socket socket) {

        if (socket == null) {
            return;
        }

        try {

            socket.close();

        } catch (IOException ignored) {
        }
    }

    private record Connection(
            Socket socket,
            BufferedReader in,
            BufferedWriter out) {}
}