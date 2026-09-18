package com.example.flink.control.model;

import java.util.List;

public class StatsResponse {

    public long totalTransactions;
    public double totalVolume;
    public long standardCount;
    public long highValueCount;
    public long overdraftCount;
    public double overdraftRatePercent;
    public double transactionsPerSecond;
    public List<AccountBalance> topAccounts;

    public static class AccountBalance {
        public String accountId;
        public double balance;

        public AccountBalance(String accountId, double balance) {
            this.accountId = accountId;
            this.balance = balance;
        }
    }
}
