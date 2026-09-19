package com.example.flink.control.service;

import com.example.flink.control.model.InterrogationReport;
import com.example.flink.control.model.TransactionView;
import com.example.flink.job.TransactionProcessor;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * Builds the "Interrogate" detail report for a single Live Feed transaction: pauses generation
 * (via {@link FlinkJobManager}) and walks the same stages {@code TransactionProcessor}'s stateful
 * map function performs, rendering each as an illustrative SQL statement plus its real result.
 */
@Service
public class InterrogationService {

    private final LiveDataStore liveDataStore;
    private final FlinkJobManager jobManager;

    public InterrogationService(LiveDataStore liveDataStore, FlinkJobManager jobManager) {
        this.liveDataStore = liveDataStore;
        this.jobManager = jobManager;
    }

    /** Pauses transaction generation and builds the report for the given transaction. */
    public InterrogationReport interrogate(long transactionId) {
        TransactionView tx = liveDataStore.findById(transactionId)
                .orElseThrow(() -> new NoSuchElementException(
                        "Transaction " + transactionId + " is no longer in the live feed buffer"));

        // Pause before reading parallelism (not before the lookup above): the transaction itself
        // is already fixed once found, but parallelism could change out from under us if we read
        // it before pausing and a Stop/Start snuck in between.
        jobManager.pauseGeneration();

        return buildReport(tx, jobManager.statusResponse().parallelism);
    }

    /** Pauses transaction generation without pinning the pause to any one transaction. */
    public void pause() {
        jobManager.pauseGeneration();
    }

    public void resume() {
        jobManager.resumeGeneration();
    }

    private InterrogationReport buildReport(TransactionView tx, int parallelism) {
        InterrogationReport report = new InterrogationReport();
        report.transaction = tx;
        report.keyId = tx.accountId;
        report.partitionField = TransactionProcessor.PARTITION_KEY_FIELD;
        report.parallelism = parallelism;
        report.maxParallelism = TransactionProcessor.defaultMaxParallelism(parallelism);
        report.keyGroup = TransactionProcessor.keyGroupForKey(tx.accountId, parallelism);
        report.assignedSubtask = TransactionProcessor.subtaskForKey(tx.accountId, parallelism);
        report.balanceBefore = tx.accountBalance;
        report.balanceAfter = tx.newBalance;
        report.classification = tx.processingStatus;

        String key = tx.accountId;
        boolean overdraft = "OVERDRAFT_WARNING".equals(tx.processingStatus);

        report.steps.add(new InterrogationReport.SqlStep(
                "1. Partition the event (keyBy)",
                "-- keyBy(\"" + report.partitionField + "\") hashes the key into one of " + report.maxParallelism
                        + " key groups,\n"
                        + "-- which are split evenly across the job's " + parallelism + " parallel subtasks.\n"
                        + "SELECT transactionId, accountId, amount, timestamp\n"
                        + "FROM transaction_stream\n"
                        + "WHERE accountId = '" + key + "'",
                String.format(Locale.US, "keyGroup=%d -> subtask %d of %d",
                        report.keyGroup, report.assignedSubtask, parallelism)));

        report.steps.add(new InterrogationReport.SqlStep(
                "2. Read current balance from keyed state",
                "-- ValueState<Double> \"accountBalance\", scoped to key = '" + key + "'\n"
                        + "SELECT COALESCE(state.balance, 0.0) AS current_balance\n"
                        + "FROM account_balance_state AS state\n"
                        + "WHERE state.key = '" + key + "'",
                String.format(Locale.US, "current_balance = %.2f", report.balanceBefore)));

        report.steps.add(new InterrogationReport.SqlStep(
                "3. Apply the transaction and update state",
                "-- runningBalance.update(currentBalance + transaction.amount)\n"
                        + "UPDATE account_balance_state\n"
                        + "SET balance = balance + (" + formatAmount(tx.amount) + ")\n"
                        + "WHERE key = '" + key + "'",
                String.format(Locale.US, "new_balance = %.2f", report.balanceAfter)));

        report.steps.add(new InterrogationReport.SqlStep(
                "4. Classify the transaction",
                "-- inline CASE logic in the stateful map function\n"
                        + "SELECT CASE\n"
                        + "    WHEN new_balance < 0 THEN 'OVERDRAFT_WARNING'\n"
                        + "    WHEN amount > 700.00 AND new_balance > 0 THEN 'HighAmountTransaction'\n"
                        + "    ELSE 'Standard'\n"
                        + "  END AS processing_status",
                "processing_status = '" + report.classification + "'"));

        report.steps.add(new InterrogationReport.SqlStep(
                "5. Route to sinks",
                "-- filter(processingStatus = 'OVERDRAFT_WARNING') feeds the webhook sink;\n"
                        + "-- every transaction also goes to the stdout sink unfiltered.\n"
                        + "SELECT * FROM processed_stream\n"
                        + "WHERE processing_status = 'OVERDRAFT_WARNING'",
                overdraft ? "matched -> Webhook Alert Sink + Stdout Console Logger" : "no match -> Stdout Console Logger only"));

        return report;
    }

    private static String formatAmount(double amount) {
        return (amount >= 0 ? "+" : "") + String.format(Locale.US, "%.2f", amount);
    }
}
