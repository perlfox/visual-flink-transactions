package com.example.flink.job;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the Data Model and the Flink pipeline building logic.
 * The core logic is isolated here.
 */

public class TransactionProcessor {

    // --- Data Model (Serializable for Flink) --
    public static class Transaction implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(TransactionProcessor.class);

        public long transactionId;
        public String accountId;
        public double amount;
        public long timestamp;
        public String processingStatus;
        public double accountBalance;
        public double newBalance;
        public String padding; // To hold junk data to bloat state

        public Transaction() {}

        public Transaction(long transactionId, String accountId, double amount, long timestamp, double accountBalance, double newBalance, boolean bloatState) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.amount = amount;
            this.timestamp = timestamp;
            this.accountBalance = accountBalance;
            this.newBalance = newBalance;

            if (bloatState) {
                // Generates ~2KB of data (2048 characters)
                this.padding = new String(new char[2048]).replace('\0', 'X');
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "Transaction{id=%d, account='%s', amount=%.2f, time=%d, Current_Balance='%.2f', New_Balance='%.2f', status='%s', padding='%s'}",
                    transactionId, accountId, amount, timestamp, accountBalance, newBalance, processingStatus, padding
            );
        }
    }

    // --- Continuous Transaction Source ---

    public static class ContinuousTransactionSource extends RichParallelSourceFunction<Transaction> {


        //This is the system exit. If you click "Cancel" or "Stop" in the UI, or if the Flink cluster needs to shut down for maintenance,
        // Flink calls the cancel() method. If you don't have isRunning in your loop, the source might keep trying to generate data for a few seconds during the shutdown process,
        // which can lead to messy logs or interrupted checkpoints.
        private volatile boolean isRunning = true;

        private long startTransactionIdAt = 200045; // Just looks more real
        private final Random random = new Random();
        private final int maxAccounts;
        private final boolean bloatState;
        private final int maxTransactionsPerSecond;
        private long perSubtaskIntervalNanos;


        public ContinuousTransactionSource(int maxAccounts, boolean bloatState, int maxTransactionsPerSecond) {
            this.maxAccounts = maxAccounts;
            this.bloatState = bloatState;
            this.maxTransactionsPerSecond = maxTransactionsPerSecond;
        }

        @Override
        public void open(Configuration parameters) {
            // Unthrottled, this loop emits as fast as the CPU allows (observed >100k tx/sec locally),
            // which floods checkpoint buffers and GC with garbage. Spread the configured system-wide
            // cap evenly across parallel subtasks so it holds regardless of parallelism.
            int subtasks = Math.max(1, getRuntimeContext().getNumberOfParallelSubtasks());
            int perSubtaskRate = Math.max(1, maxTransactionsPerSecond / subtasks);
            perSubtaskIntervalNanos = 1_000_000_000L / perSubtaskRate;
        }

        @Override
        public void run(SourceFunction.SourceContext<Transaction> ctx) throws Exception {
            long nextEmitAtNanos = System.nanoTime();
            while (isRunning) {
                if (TransactionSourceControl.isPaused()) {
                    // Interrogate mode: freeze generation entirely. Re-anchor the emit clock on
                    // resume so we don't burst-emit a backlog of "missed" transactions.
                    LockSupport.parkNanos(50_000_000L);
                    nextEmitAtNanos = System.nanoTime();
                    continue;
                }

                long waitNanos = nextEmitAtNanos - System.nanoTime();
                if (waitNanos > 0) {
                    LockSupport.parkNanos(waitNanos);
                    continue;
                }
                nextEmitAtNanos += perSubtaskIntervalNanos;

                // Generate new transaction data
                long txId = startTransactionIdAt++;

                // ensures that every randomAccountNum generated will be a unique-looking 5-digit ID starting with the number 1 (from 10,000 to 13,999)
                // basically 4000 different account possibilities
                int randomAccountNum = 10000 + random.nextInt(maxAccounts);
                String accountId = "ACCT-" + randomAccountNum;

                // Generate a random amount for a transaction between 1.00 and 1000.00 for base amount
                double baseTransactionAmount = 1.0 + (1000.0 - 1.0) * random.nextDouble();

                // Introduce a chance for the amount to be negative (e.g., 20% chance)
                double amount;
                if (random.nextDouble() < 0.20) {
                    // 20% chance: make it negative (withdrawal/refund)
                    amount = -baseTransactionAmount;
                } else {
                    // 80% chance: keep it positive (deposit/credit)
                    amount = baseTransactionAmount;
                }

                long timestamp = System.currentTimeMillis();

                Transaction newTransaction = new Transaction(txId, accountId, amount, timestamp, 0.00, 0.00, bloatState);

                // Emit the transaction with a synchronized lock
                // This lock is important for correct checkpointing
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(newTransaction);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static final String DEFAULT_WEBHOOK_URL = "https://webhook.site/a1b731f8-6003-41f0-948a-6dd9c8c3fa3f";

    /** Field the pipeline partitions on via keyBy; every transaction for a given value is routed to the same subtask. */
    public static final String PARTITION_KEY_FIELD = "accountId";

    /** Caps the unthrottled source's output so it can't flood the pipeline with garbage/checkpoint data. */
    public static final int DEFAULT_MAX_TRANSACTIONS_PER_SECOND = 1000;

    /**
     * The max parallelism Flink assigns a keyed stream by default when none is set explicitly on
     * the job, given its operator parallelism. keyBy hashes every key into one of this many key
     * groups, which are then split evenly across the parallel subtasks.
     */
    public static int defaultMaxParallelism(int parallelism) {
        return KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
    }

    /** Which key group {@code keyBy(accountId)} would hash the given key into. */
    public static int keyGroupForKey(String key, int parallelism) {
        return KeyGroupRangeAssignment.assignToKeyGroup(key, defaultMaxParallelism(parallelism));
    }

    /** Which parallel subtask (of {@code parallelism}) {@code keyBy(accountId)} routes the given key to. */
    public static int subtaskForKey(String key, int parallelism) {
        return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, defaultMaxParallelism(parallelism), parallelism);
    }

    public static void execute(StreamExecutionEnvironment env, String jobName, int maxAccounts, boolean bloatState) throws Exception {
        execute(env, jobName, maxAccounts, bloatState, false, DEFAULT_WEBHOOK_URL, DEFAULT_MAX_TRANSACTIONS_PER_SECOND);
    }

    public static void execute(StreamExecutionEnvironment env, String jobName, int maxAccounts, boolean bloatState,
                                boolean webhookEnabled, String webhookUrl) throws Exception {
        execute(env, jobName, maxAccounts, bloatState, webhookEnabled, webhookUrl, DEFAULT_MAX_TRANSACTIONS_PER_SECOND);
    }

    public static void execute(StreamExecutionEnvironment env, String jobName, int maxAccounts, boolean bloatState,
                                boolean webhookEnabled, String webhookUrl, int maxTransactionsPerSecond) throws Exception {
        buildPipeline(env, maxAccounts, bloatState, webhookEnabled, webhookUrl, maxTransactionsPerSecond);

        // Execute the Flink job
        Log.info("Starting Flink Job Execution...");
        env.execute(jobName);
        Log.info("Flink Job Started and Running Continuously...");
    }

    /**
     * Builds the pipeline graph on the given environment without executing it, so callers
     * that need a JobClient (e.g. to cancel the job later) can call env.executeAsync(...) themselves.
     */
    public static void buildPipeline(StreamExecutionEnvironment env, int maxAccounts, boolean bloatState,
                                      boolean webhookEnabled, String webhookUrl, int maxTransactionsPerSecond) {

        // Read data from the new Continuous Source
        // This is now an UNBOUNDED source, meaning the job will never finish.
        DataStream<Transaction> transactionStream = env
                .addSource(new ContinuousTransactionSource(maxAccounts, bloatState, maxTransactionsPerSecond))
                .name("Continuous Transaction Source")
                .uid("source-001");;

        // Add the new columns (processingStatus, currentBalance, and NewBalance) using a RichMapFunction
        // Use RichMapFunction over Mapfunction since this uses state. MapFunction can not see the Runtime Context
        DataStream<Transaction> processedStream = transactionStream
                .keyBy(transaction -> transaction.accountId) // Group by account
                .map(new RichMapFunction<Transaction, Transaction>() {

                    // The state handle: Flink manages this for us
                    private transient ValueState<Double> runningBalance;

                    @Override
                    public void open(Configuration parameters) {
                        // Initialize the state descriptor
                        ValueStateDescriptor<Double> descriptor =
                                new ValueStateDescriptor<>("accountBalance", Double.class, 0.0);
                        runningBalance = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Transaction map(Transaction transaction) throws Exception {
                        // Get the current balance from state
                        Double currentBalance = runningBalance.value();
                        if (currentBalance == null) { currentBalance = 0.0; }

                        // Update the balance with the new transaction amount
                        double newBalance = currentBalance + transaction.amount;
                        runningBalance.update(newBalance);

                        // Add logic using that state (Example: Status Flag if account goes negative or an expensive purchase)
                        if (newBalance < 0) {
                            transaction.processingStatus = "OVERDRAFT_WARNING";
                        } else if (transaction.amount > 700.00 && newBalance > 0) {
                            transaction.processingStatus = "HighAmountTransaction";
                        } else {
                            transaction.processingStatus = "Standard";
                        }

                        transaction.newBalance = newBalance;
                        transaction.accountBalance = currentBalance;

                        // Optional: Print balance to console for debugging
                        // System.out.println("Account: " + transaction.accountId + " | New Balance: " + newBalance);

                        // In-process fan-out for embedded (control-service) use; no-op otherwise.
                        TransactionEventBus.publish(transaction);

                        return transaction;
                    }
                })
                .returns(Transaction.class)
                .name("Stateful Account Balance Tracker & Fraud Logic")
                .uid("map-001");

        DataStream<Transaction> alertsOnly = processedStream
                .filter(t -> "OVERDRAFT_WARNING".equals(t.processingStatus))
                .name("Filter: Overdraft")
                .uid("filter-alerts-001");

        alertsOnly
                .addSink(new WebhookSink(webhookEnabled, webhookUrl))
                .name("Webhook Alert Sink")
                .uid("sink-webhook-001");

        // Output to STDOUT
        processedStream
                .print("Bank Transaction")
                .name("Stdout Console Logger")
                .uid("sink-stdout-001");
    }

}


