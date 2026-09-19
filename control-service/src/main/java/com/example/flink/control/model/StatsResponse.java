package com.example.flink.control.model;

import java.util.List;

/** GET /api/stats response body: live aggregates over the currently (or most recently) running job. */
public class StatsResponse {

    public long totalTransactions;
    public double totalVolume;
    public long standardCount;
    public long highValueCount;
    public long overdraftCount;
    public double overdraftRatePercent;
    public double transactionsPerSecond;

    /** Top {@code TOP_ACCOUNTS} accounts by absolute balance, descending. */
    public List<AccountBalance> topAccounts;

    /** One account's current balance, as of its most recent processed transaction. */
    public static class AccountBalance {
        public String accountId;
        public double balance;

        public AccountBalance(String accountId, double balance) {
            this.accountId = accountId;
            this.balance = balance;
        }
    }
}
