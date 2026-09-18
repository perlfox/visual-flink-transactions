package com.example.flink.control.service;

import com.example.flink.control.model.StatsResponse;
import com.example.flink.control.model.TransactionView;
import com.example.flink.job.TransactionProcessor;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Consumer;

/**
 * In-memory rolling buffer + live aggregates for the currently (or most recently) running job.
 * No persistence: state is cleared each time a new job run starts.
 */
@Service
public class LiveDataStore {

    private static final int MAX_BUFFERED = 5000;
    private static final int TOP_ACCOUNTS = 10;

    private final Object bufferLock = new Object();
    private final Deque<TransactionView> buffer = new ArrayDeque<>(MAX_BUFFERED);

    private final AtomicLong totalTransactions = new AtomicLong();
    private final AtomicLong standardCount = new AtomicLong();
    private final AtomicLong highValueCount = new AtomicLong();
    private final AtomicLong overdraftCount = new AtomicLong();
    private final AtomicLong currentSecondCount = new AtomicLong();
    private final DoubleAdder totalVolume = new DoubleAdder();
    private volatile double transactionsPerSecond = 0.0;

    private final Map<String, Double> accountBalances = new ConcurrentHashMap<>();
    private final List<Consumer<TransactionView>> subscribers = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService rateTicker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "tx-rate-ticker");
        t.setDaemon(true);
        return t;
    });

    public LiveDataStore() {
        rateTicker.scheduleAtFixedRate(
                () -> transactionsPerSecond = currentSecondCount.getAndSet(0),
                1, 1, TimeUnit.SECONDS);
    }

    public void record(TransactionProcessor.Transaction transaction) {
        TransactionView view = TransactionView.from(transaction);

        synchronized (bufferLock) {
            if (buffer.size() >= MAX_BUFFERED) {
                buffer.removeFirst();
            }
            buffer.addLast(view);
        }

        totalTransactions.incrementAndGet();
        currentSecondCount.incrementAndGet();
        totalVolume.add(view.amount);
        accountBalances.put(view.accountId, view.newBalance);

        switch (view.processingStatus) {
            case "OVERDRAFT_WARNING" -> overdraftCount.incrementAndGet();
            case "HighAmountTransaction" -> highValueCount.incrementAndGet();
            default -> standardCount.incrementAndGet();
        }

        for (Consumer<TransactionView> subscriber : subscribers) {
            subscriber.accept(view);
        }
    }

    public void subscribe(Consumer<TransactionView> subscriber) {
        subscribers.add(subscriber);
    }

    public void unsubscribe(Consumer<TransactionView> subscriber) {
        subscribers.remove(subscriber);
    }

    public List<TransactionView> recent(int limit) {
        synchronized (bufferLock) {
            List<TransactionView> all = new ArrayList<>(buffer);
            int fromIndex = Math.max(0, all.size() - limit);
            return new ArrayList<>(all.subList(fromIndex, all.size()));
        }
    }

    public StatsResponse stats() {
        StatsResponse response = new StatsResponse();
        long total = totalTransactions.get();
        response.totalTransactions = total;
        response.totalVolume = totalVolume.sum();
        response.standardCount = standardCount.get();
        response.highValueCount = highValueCount.get();
        response.overdraftCount = overdraftCount.get();
        response.overdraftRatePercent = total == 0 ? 0.0 : (overdraftCount.get() * 100.0) / total;
        response.transactionsPerSecond = transactionsPerSecond;
        response.topAccounts = accountBalances.entrySet().stream()
                .sorted(Comparator.comparingDouble((Map.Entry<String, Double> e) -> Math.abs(e.getValue())).reversed())
                .limit(TOP_ACCOUNTS)
                .map(e -> new StatsResponse.AccountBalance(e.getKey(), e.getValue()))
                .toList();
        return response;
    }

    /** Clears all buffered data and aggregates; called when a new job run starts. */
    public void reset() {
        synchronized (bufferLock) {
            buffer.clear();
        }
        totalTransactions.set(0);
        standardCount.set(0);
        highValueCount.set(0);
        overdraftCount.set(0);
        currentSecondCount.set(0);
        totalVolume.reset();
        transactionsPerSecond = 0.0;
        accountBalances.clear();
    }
}
