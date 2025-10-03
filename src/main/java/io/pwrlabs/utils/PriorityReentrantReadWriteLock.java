package io.pwrlabs.utils;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PriorityReentrantReadWriteLock {
    public enum priority {HIGH, MEDIUM, LOW};

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    private final LinkedBlockingQueue<LockRequest> highPriorityQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<LockRequest> mediumPriorityQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<LockRequest> lowPriorityQueue = new LinkedBlockingQueue<>();
    private final Semaphore pending = new Semaphore(0);
    private final AtomicLong currentWriteLockHolders = new AtomicLong(0);
    private final AtomicInteger writeLockReentranceCount = new AtomicInteger(0);

    public PriorityReentrantReadWriteLock() {
        initQueueProcessor();
    }

    private void initQueueProcessor() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    LockRequest f = highPriorityQueue.poll();
                    if(f == null) f = mediumPriorityQueue.poll();
                    if(f == null) f = lowPriorityQueue.poll();

                    if(f != null) {
                        if(!f.canProceed.isCancelled())
                            f.canProceed.complete(null);

                        if(!f.canProceed.isCancelled()) {
                            f.lockAcquired.get();
                        }
                    } else {
                        pending.acquire();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        thread.setDaemon(true);
        thread.setName("PriorityReentrantReadWriteLock-QueueProcessor");
        thread.start();
    }

    public void acquireWriteLock(priority p, long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
         if(currentWriteLockHolders.get() == Thread.currentThread().getId()) {
             writeLockReentranceCount.incrementAndGet();
             return;
         }

         LockRequest request = new LockRequest();
        switch (p) {
            case HIGH -> highPriorityQueue.put(request);
            case MEDIUM -> mediumPriorityQueue.put(request);
            case LOW -> lowPriorityQueue.put(request);
        }
        pending.release();

        try {
            request.canProceed.get(timeoutMs, TimeUnit.MILLISECONDS);
            readWriteLock.writeLock().lock();
            request.lockAcquired.complete(null);
            currentWriteLockHolders.set(Thread.currentThread().getId());
            writeLockReentranceCount.set(1);
        } finally {
            request.canProceed.cancel(true);
        }
    }

    public void releaseWriteLock() {
        if(currentWriteLockHolders.get() != Thread.currentThread().getId()) {
            throw new IllegalStateException("Current thread does not hold the write lock");
        }

        if (writeLockReentranceCount.addAndGet(-1) == 0) {
            currentWriteLockHolders.set(0);
            readWriteLock.writeLock().unlock();
        }
    }

    public void acquireReadLock() {
        readWriteLock.readLock().lock();
    }

    public void releaseReadLock() {
        readWriteLock.readLock().unlock();
    }

    public boolean isHeldByCurrentThread() {
        return currentWriteLockHolders.get() == Thread.currentThread().getId();
    }

    public static void main(String[] args) throws Exception {
        PriorityReentrantReadWriteLock lock = new PriorityReentrantReadWriteLock();

        lock.acquireReadLock();

        lock.acquireWriteLock(priority.HIGH, 1000);

        lock.releaseReadLock();

        System.out.println("sdsd");
    }

    private static class LockRequest {
        final CompletableFuture<Void> canProceed = new CompletableFuture<>();
        final CompletableFuture<Void> lockAcquired = new CompletableFuture<>();
    }
}
