package com.example.flink.control.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Detailed, single-transaction explanation for the Live Feed's "Interrogate" mode: which key the
 * transaction was routed on, where it landed, and (as illustrative SQL, since this pipeline is a
 * plain DataStream job with no Table/SQL API involved) the equivalent query for each stage of the
 * stateful logic in {@code TransactionProcessor}, alongside that stage's real result.
 */
public class InterrogationReport {

    public TransactionView transaction;

    /** The keyBy value this transaction was partitioned on (== transaction.accountId). */
    public String keyId;
    public String partitionField;

    public int parallelism;
    public int maxParallelism;
    public int keyGroup;
    public int assignedSubtask;

    public double balanceBefore;
    public double balanceAfter;
    public String classification;

    public final List<SqlStep> steps = new ArrayList<>();

    public static class SqlStep {
        public String title;
        public String sql;
        public String result;

        public SqlStep() {}

        public SqlStep(String title, String sql, String result) {
            this.title = title;
            this.sql = sql;
            this.result = result;
        }
    }
}
