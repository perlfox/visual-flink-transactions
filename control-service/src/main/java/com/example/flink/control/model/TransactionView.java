package com.example.flink.control.model;

import com.example.flink.job.TransactionProcessor;

/** GUI-facing projection of TransactionProcessor.Transaction, without the state-bloat padding field. */
public class TransactionView {

    public long transactionId;
    public String accountId;
    public double amount;
    public long timestamp;
    public String processingStatus;
    public double newBalance;

    public static TransactionView from(TransactionProcessor.Transaction t) {
        TransactionView view = new TransactionView();
        view.transactionId = t.transactionId;
        view.accountId = t.accountId;
        view.amount = t.amount;
        view.timestamp = t.timestamp;
        view.processingStatus = t.processingStatus;
        view.newBalance = t.newBalance;
        return view;
    }
}
