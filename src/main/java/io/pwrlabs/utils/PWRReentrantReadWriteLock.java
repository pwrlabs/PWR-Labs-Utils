package io.pwrlabs.utils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static io.pwrlabs.newerror.NewError.errorIf;

/**
 * A priority-based reentrant read-write lock implementation that supports priority queuing
 * and timeout functionality. Higher priority requests are granted locks first, and within
 * the same priority level, latest requests have higher priority (LIFO).
 */
public class PWRReentrantReadWriteLock {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PWRReentrantReadWriteLock.class);
    public static final int PRIORITY_LOW = 1;
    public static final int PRIORITY_MEDIUM = 5;
    public static final int PRIORITY_HIGH = 10;

    //region ==================== Fields ========================
    private final String lockName;
    private final long unhealthyWriteLockAcquireMs;

    /**
     * Main lock for coordinating access
     */
    private final ReentrantLock mainLock = new ReentrantLock();

    /**
     * Priority queue for waiting threads
     */
    private final PriorityBlockingQueue<WaitingThread> waitingQueue;

    /**
     * Map to track conditions for each waiting thread
     */
    private final Map<Thread, Condition> threadConditions = new ConcurrentHashMap<>();

    /**
     * Counter for request ordering within same priority
     */
    private final AtomicLong requestCounter = new AtomicLong(0);

    /**
     * Current state tracking
     */
    private final AtomicInteger activeReaders = new AtomicInteger(0);
    private volatile Thread writeLockOwner = null;
    private final AtomicInteger writeReentrantCount = new AtomicInteger(0);
    private volatile long writeLockTime = 0;

    /**
     * Map to track read lock count per thread (for reentrancy)
     */
    private final Map<Thread, Integer> readHoldCounts = new ConcurrentHashMap<>();
    //endregion

    //region ==================== Inner Classes ========================
    /**
     * Represents a thread waiting for a lock with its priority and request time
     */
    private static class WaitingThread implements Comparable<WaitingThread> {
        final Thread thread;
        final int priority;
        final long requestOrder; // For LIFO within same priority
        final boolean isWriteRequest;

        WaitingThread(Thread thread, int priority, long requestOrder, boolean isWriteRequest) {
            this.thread = thread;
            this.priority = priority;
            this.requestOrder = requestOrder;
            this.isWriteRequest = isWriteRequest;
        }

        @Override
        public int compareTo(WaitingThread other) {
            // Higher priority first
            int priorityCompare = Integer.compare(other.priority, this.priority);
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            // For same priority, latest request first (LIFO - higher requestOrder first)
            return Long.compare(other.requestOrder, this.requestOrder);
        }
    }
    //endregion

    //region ==================== Constructor ========================
    public PWRReentrantReadWriteLock(String lockName, long unhealthyWriteLockAcquireMs) {
        this.lockName = lockName;
        this.unhealthyWriteLockAcquireMs = unhealthyWriteLockAcquireMs;
        this.waitingQueue = new PriorityBlockingQueue<>();
    }
    //endregion

    //region ==================== Public Getters ========================
    public Thread getWriteLockThread() {
        return writeLockOwner;
    }

    public long getWriteLockTime() {
        return writeLockTime;
    }

    public int getReadLockCount() {
        return activeReaders.get();
    }

    public int getWriteLockCount() {
        return writeReentrantCount.get();
    }

    public boolean isHeldByCurrentThread() {
        return writeLockOwner == Thread.currentThread();
    }
    //endregion

    //region ==================== Read Lock Methods ========================
    /**
     * Acquires the read lock with specified priority and timeout.
     *
     * @param priority Higher values indicate higher priority
     * @param timeout Maximum time to wait
     * @param unit Time unit for timeout
     * @return true if lock was acquired, false if timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean acquireReadLock(int priority, long timeout, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        long nanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanos;

        mainLock.lock();
        try {
            // Check for read lock reentrancy
            Integer holdCount = readHoldCounts.get(currentThread);
            if (holdCount != null && holdCount > 0) {
                readHoldCounts.put(currentThread, holdCount + 1);
                activeReaders.incrementAndGet();
                return true;
            }

            // Check if we can acquire immediately
            if (canGrantReadLock(currentThread)) {
                grantReadLock(currentThread);
                return true;
            }

            // Add to waiting queue
            WaitingThread waitingThread = new WaitingThread(
                    currentThread, priority, requestCounter.incrementAndGet(), false
            );
            waitingQueue.offer(waitingThread);

            Condition condition = mainLock.newCondition();
            threadConditions.put(currentThread, condition);

            try {
                while (!canGrantReadLock(currentThread)) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        // Timeout occurred
                        waitingQueue.remove(waitingThread);
                        return false;
                    }

                    // Check if this thread is next in priority queue
                    if (!isNextInQueue(currentThread)) {
                        condition.awaitNanos(remaining);
                    } else {
                        condition.awaitNanos(remaining);
                        if (canGrantReadLock(currentThread)) {
                            waitingQueue.remove(waitingThread);
                            grantReadLock(currentThread);
                            return true;
                        }
                    }
                }

                waitingQueue.remove(waitingThread);
                grantReadLock(currentThread);
                return true;

            } finally {
                threadConditions.remove(currentThread);
            }

        } finally {
            signalWaiters();
            mainLock.unlock();
        }
    }

    /**
     * Convenience method - acquires read lock without timeout
     */
    public void acquireReadLock(int priority) throws InterruptedException {
        acquireReadLock(priority, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Convenience method - acquires read lock without timeout
     */
    public void acquireReadLock() throws InterruptedException {
        acquireReadLock(PRIORITY_MEDIUM, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Releases the read lock.
     */
    public void releaseReadLock() {
        Thread currentThread = Thread.currentThread();

        mainLock.lock();
        try {
            Integer holdCount = readHoldCounts.get(currentThread);
            errorIf(holdCount == null || holdCount == 0,
                    "Current thread does not hold read lock");

            if (holdCount == 1) {
                readHoldCounts.remove(currentThread);
            } else {
                readHoldCounts.put(currentThread, holdCount - 1);
            }

            int newCount = activeReaders.decrementAndGet();
            if (newCount == 0) {
                signalWaiters();
            }
        } finally {
            mainLock.unlock();
        }
    }
    //endregion

    //region ==================== Write Lock Methods ========================
    /**
     * Acquires the write lock with specified priority and timeout.
     *
     * @param priority Higher values indicate higher priority
     * @param timeout Maximum time to wait
     * @param unit Time unit for timeout
     * @return true if lock was acquired, false if timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean acquireWriteLock(int priority, long timeout, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        long startTime = System.currentTimeMillis();
        long nanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanos;

        mainLock.lock();
        try {
            // Check for write lock reentrancy
            if (writeLockOwner == currentThread) {
                writeReentrantCount.incrementAndGet();
                return true;
            }

            // Check if we can acquire immediately
            if (canGrantWriteLock()) {
                grantWriteLock(currentThread);
                long endTime = System.currentTimeMillis();
                if (endTime - startTime >= unhealthyWriteLockAcquireMs) {
                    logger.error("acquireWriteLock took {} ms", (endTime - startTime));
                }
                return true;
            }

            // Add to waiting queue
            WaitingThread waitingThread = new WaitingThread(
                    currentThread, priority, requestCounter.incrementAndGet(), true
            );
            waitingQueue.offer(waitingThread);

            Condition condition = mainLock.newCondition();
            threadConditions.put(currentThread, condition);

            try {
                while (!canGrantWriteLock()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        // Timeout occurred
                        waitingQueue.remove(waitingThread);
                        return false;
                    }

                    // Check if this thread is next in priority queue
                    if (!isNextInQueue(currentThread)) {
                        condition.awaitNanos(remaining);
                    } else {
                        condition.awaitNanos(remaining);
                        if (canGrantWriteLock()) {
                            waitingQueue.remove(waitingThread);
                            grantWriteLock(currentThread);
                            long endTime = System.currentTimeMillis();
                            if (endTime - startTime >= unhealthyWriteLockAcquireMs) {
                                logger.error("acquireWriteLock took {} ms", (endTime - startTime));
                            }
                            return true;
                        }
                    }
                }

                waitingQueue.remove(waitingThread);
                grantWriteLock(currentThread);
                long endTime = System.currentTimeMillis();
                if (endTime - startTime >= unhealthyWriteLockAcquireMs) {
                    logger.error("acquireWriteLock took {} ms", (endTime - startTime));
                }
                return true;

            } finally {
                threadConditions.remove(currentThread);
            }

        } finally {
            signalWaiters();
            mainLock.unlock();
        }
    }

    /**
     * Convenience method - acquires write lock without timeout
     */
    public void acquireWriteLock(int priority) throws InterruptedException {
        acquireWriteLock(priority, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    /**
     * Releases the write lock.
     */
    public void releaseWriteLock() {
        Thread currentThread = Thread.currentThread();

        mainLock.lock();
        try {
            errorIf(writeLockOwner == null, "Write lock is not held by any thread");
            errorIf(writeLockOwner != currentThread, "Current thread does not hold the write lock");

            if (writeReentrantCount.decrementAndGet() == 0) {
                writeLockOwner = null;
                writeLockTime = 0;
                signalWaiters();
            }
        } finally {
            mainLock.unlock();
        }
    }

    /**
     * Attempts to acquire write lock without blocking.
     *
     * @return true if lock was acquired, false otherwise
     */
    public boolean tryToAcquireWriteLock() {
        Thread currentThread = Thread.currentThread();

        mainLock.lock();
        try {
            if (writeLockOwner == currentThread) {
                writeReentrantCount.incrementAndGet();
                return true;
            }

            if (canGrantWriteLock()) {
                grantWriteLock(currentThread);
                return true;
            }

            return false;
        } finally {
            mainLock.unlock();
        }
    }
    //endregion

    //region ==================== Private Helper Methods ========================
    private boolean canGrantReadLock(Thread thread) {
        // Can grant read lock if no writer and no writer waiting with higher priority
        if (writeLockOwner != null && writeLockOwner != thread) {
            return false;
        }

        // Check if there's a higher priority write request waiting
        for (WaitingThread waiting : waitingQueue) {
            if (waiting.thread == thread) {
                continue;
            }
            if (waiting.isWriteRequest) {
                // There's a write request waiting, can't grant read lock
                return false;
            }
        }

        return true;
    }

    private boolean canGrantWriteLock() {
        return writeLockOwner == null && activeReaders.get() == 0;
    }

    private void grantReadLock(Thread thread) {
        activeReaders.incrementAndGet();
        readHoldCounts.merge(thread, 1, Integer::sum);
    }

    private void grantWriteLock(Thread thread) {
        writeLockOwner = thread;
        writeLockTime = System.nanoTime();
        writeReentrantCount.set(1);
    }

    private boolean isNextInQueue(Thread thread) {
        WaitingThread peek = waitingQueue.peek();
        return peek != null && peek.thread == thread;
    }

    private void signalWaiters() {
        // Signal all waiting threads to re-check conditions
        // They will check their priority order
        for (Condition condition : threadConditions.values()) {
            condition.signal();
        }
    }
    //endregion
}