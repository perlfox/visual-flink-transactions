package com.example.flink.job;

/**
 * In-process pause switch for {@link TransactionProcessor.ContinuousTransactionSource}, so the
 * control-service's "Interrogate" mode can freeze transaction generation (to let a user inspect a
 * single transaction in the live feed) without cancelling the running job. Mirrors
 * {@link TransactionEventBus}'s static in-process pattern; a no-op unless something toggles it.
 */
public final class TransactionSourceControl {

    private static volatile boolean paused = false;

    private TransactionSourceControl() {}

    public static void pause() {
        paused = true;
    }

    public static void resume() {
        paused = false;
    }

    public static boolean isPaused() {
        return paused;
    }
}
