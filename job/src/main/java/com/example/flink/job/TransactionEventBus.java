package com.example.flink.job;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-process fan-out of processed transactions to any listeners running in the same JVM.
 * Used by the control-service to observe an embedded local-MiniCluster job without Kafka/DB.
 * A no-op when nothing is listening, so standalone/external-cluster runs are unaffected.
 */
public final class TransactionEventBus {

    public interface TransactionListener {
        void onTransaction(TransactionProcessor.Transaction transaction);
    }

    private static final List<TransactionListener> LISTENERS = new CopyOnWriteArrayList<>();

    private TransactionEventBus() {}

    public static void subscribe(TransactionListener listener) {
        LISTENERS.add(listener);
    }

    public static void unsubscribe(TransactionListener listener) {
        LISTENERS.remove(listener);
    }

    public static void publish(TransactionProcessor.Transaction transaction) {
        for (TransactionListener listener : LISTENERS) {
            listener.onTransaction(transaction);
        }
    }
}
