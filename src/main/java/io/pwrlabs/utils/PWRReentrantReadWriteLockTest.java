package io.pwrlabs.utils;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.SAME_THREAD)
class PWRReentrantReadWriteLockTest {
    private static final Logger logger = LoggerFactory.getLogger(PWRReentrantReadWriteLockTest.class);
    private static final int TEST_TIMEOUT_MS = 5000;

    private PWRReentrantReadWriteLock lock;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        lock = new PWRReentrantReadWriteLock("testLock", 100);
        executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    //region ==================== Basic Functionality Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testBasicReadLockAcquisitionAndRelease() throws InterruptedException {
        assertFalse(lock.isHeldByCurrentThread());
        assertEquals(0, lock.getReadLockCount());

        lock.acquireReadLock(5);
        assertEquals(1, lock.getReadLockCount());

        lock.releaseReadLock();
        assertEquals(0, lock.getReadLockCount());
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testBasicWriteLockAcquisitionAndRelease() throws InterruptedException {
        assertNull(lock.getWriteLockThread());
        assertEquals(0, lock.getWriteLockCount());

        lock.acquireWriteLock(5);
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());
        assertEquals(1, lock.getWriteLockCount());
        assertTrue(lock.isHeldByCurrentThread());
        assertTrue(lock.getWriteLockTime() > 0);

        lock.releaseWriteLock();
        assertNull(lock.getWriteLockThread());
        assertEquals(0, lock.getWriteLockCount());
        assertFalse(lock.isHeldByCurrentThread());
        assertEquals(0, lock.getWriteLockTime());
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testMultipleReadersSimultaneously() throws Exception {
        int numReaders = 5;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch acquiredLatch = new CountDownLatch(numReaders);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        AtomicInteger maxConcurrentReaders = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numReaders; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    lock.acquireReadLock(5);

                    // Update max concurrent readers
                    int current = lock.getReadLockCount();
                    maxConcurrentReaders.updateAndGet(max -> Math.max(max, current));

                    acquiredLatch.countDown();
                    releaseLatch.await();
                    lock.releaseReadLock();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        startLatch.countDown();
        assertTrue(acquiredLatch.await(2, TimeUnit.SECONDS));
        assertEquals(numReaders, lock.getReadLockCount());
        assertEquals(numReaders, maxConcurrentReaders.get());

        releaseLatch.countDown();
        for (Future<?> future : futures) {
            future.get(2, TimeUnit.SECONDS);
        }

        assertEquals(0, lock.getReadLockCount());
    }

    //endregion

    //region ==================== Priority Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testPriorityOrderingForWriteLocks() throws Exception {
        List<Integer> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch allStarted = new CountDownLatch(3);

        // First thread takes the write lock
        Future<?> holder = executor.submit(() -> {
            try {
                lock.acquireWriteLock(1);
                allStarted.await(); // Wait for all threads to start
                Thread.sleep(200);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(50); // Ensure first thread gets the lock

        // Low priority waiter
        Future<?> lowPriority = executor.submit(() -> {
            try {
                allStarted.countDown();
                lock.acquireWriteLock(1); // Low priority
                acquisitionOrder.add(1);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(50); // Ensure low priority starts waiting

        // High priority waiter
        Future<?> highPriority = executor.submit(() -> {
            try {
                allStarted.countDown();
                lock.acquireWriteLock(10); // High priority
                acquisitionOrder.add(10);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(50); // Ensure high priority starts waiting

        // Medium priority waiter
        Future<?> mediumPriority = executor.submit(() -> {
            try {
                allStarted.countDown();
                lock.acquireWriteLock(5); // Medium priority
                acquisitionOrder.add(5);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        holder.get(3, TimeUnit.SECONDS);
        highPriority.get(3, TimeUnit.SECONDS);
        mediumPriority.get(3, TimeUnit.SECONDS);
        lowPriority.get(3, TimeUnit.SECONDS);

        assertEquals(Arrays.asList(10, 5, 1), acquisitionOrder);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testLIFOWithinSamePriority() throws Exception {
        List<Integer> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch allStarted = new CountDownLatch(3);

        // First thread takes the write lock
        Future<?> holder = executor.submit(() -> {
            try {
                lock.acquireWriteLock(1);
                allStarted.await();
                Thread.sleep(200);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread.sleep(50);

        // Three threads with same priority, arriving in order 1, 2, 3
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                try {
                    allStarted.countDown();
                    lock.acquireWriteLock(5); // Same priority for all
                    acquisitionOrder.add(threadId);
                    lock.releaseWriteLock();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            Thread.sleep(50); // Ensure ordering
        }

        holder.get(3, TimeUnit.SECONDS);
        for (Future<?> future : futures) {
            future.get(3, TimeUnit.SECONDS);
        }

        // Should be LIFO: 3, 2, 1
        assertEquals(Arrays.asList(3, 2, 1), acquisitionOrder);
    }

    //endregion

    //region ==================== Timeout Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testReadLockTimeout() throws Exception {
        // Hold write lock in another thread
        CountDownLatch lockAcquired = new CountDownLatch(1);
        Future<?> holder = executor.submit(() -> {
            try {
                lock.acquireWriteLock(5);
                lockAcquired.countDown();
                Thread.sleep(1000);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        lockAcquired.await();

        // Try to acquire read lock with timeout
        long start = System.currentTimeMillis();
        boolean acquired = lock.acquireReadLock(3, 200, TimeUnit.MILLISECONDS);
        long duration = System.currentTimeMillis() - start;

        assertFalse(acquired);
        assertTrue(duration >= 180 && duration < 400,
                "Timeout duration was " + duration + "ms");

        holder.cancel(true);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testWriteLockTimeout() throws Exception {
        // Hold read lock in another thread
        CountDownLatch lockAcquired = new CountDownLatch(1);
        Future<?> holder = executor.submit(() -> {
            try {
                lock.acquireReadLock(5);
                lockAcquired.countDown();
                Thread.sleep(1000);
                lock.releaseReadLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        lockAcquired.await();

        // Try to acquire write lock with timeout
        long start = System.currentTimeMillis();
        boolean acquired = lock.acquireWriteLock(3, 200, TimeUnit.MILLISECONDS);
        long duration = System.currentTimeMillis() - start;

        assertFalse(acquired);
        assertTrue(duration >= 180 && duration < 400,
                "Timeout duration was " + duration + "ms");

        holder.cancel(true);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testSuccessfulAcquisitionWithTimeout() throws InterruptedException {
        boolean acquired = lock.acquireWriteLock(5, 1, TimeUnit.SECONDS);
        assertTrue(acquired);
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());
        lock.releaseWriteLock();

        acquired = lock.acquireReadLock(5, 1, TimeUnit.SECONDS);
        assertTrue(acquired);
        assertEquals(1, lock.getReadLockCount());
        lock.releaseReadLock();
    }

    //endregion

    //region ==================== Reentrancy Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testWriteLockReentrancy() throws InterruptedException {
        lock.acquireWriteLock(5);
        assertEquals(1, lock.getWriteLockCount());
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());

        lock.acquireWriteLock(3); // Reentrant acquisition
        assertEquals(2, lock.getWriteLockCount());
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());

        lock.releaseWriteLock();
        assertEquals(1, lock.getWriteLockCount());
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());

        lock.releaseWriteLock();
        assertEquals(0, lock.getWriteLockCount());
        assertNull(lock.getWriteLockThread());
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testReadLockReentrancy() throws InterruptedException {
        lock.acquireReadLock(5);
        assertEquals(1, lock.getReadLockCount());

        lock.acquireReadLock(3); // Reentrant acquisition
        assertEquals(2, lock.getReadLockCount());

        lock.releaseReadLock();
        assertEquals(1, lock.getReadLockCount());

        lock.releaseReadLock();
        assertEquals(0, lock.getReadLockCount());
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testReadLockReentrancyAcrossThreads() throws Exception {
        int threadsPerGroup = 3;
        int reentrancyDepth = 3;
        CountDownLatch allAcquired = new CountDownLatch(threadsPerGroup);
        CountDownLatch proceedToRelease = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadsPerGroup; i++) {
            futures.add(executor.submit(() -> {
                try {
                    // Acquire multiple times
                    for (int j = 0; j < reentrancyDepth; j++) {
                        lock.acquireReadLock(5);
                    }

                    allAcquired.countDown();
                    proceedToRelease.await();

                    // Release multiple times
                    for (int j = 0; j < reentrancyDepth; j++) {
                        lock.releaseReadLock();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                }
            }));
        }

        assertTrue(allAcquired.await(2, TimeUnit.SECONDS));
        assertEquals(threadsPerGroup * reentrancyDepth, lock.getReadLockCount());

        proceedToRelease.countDown();
        for (Future<?> future : futures) {
            future.get(2, TimeUnit.SECONDS);
        }

        assertEquals(0, errors.get());
        assertEquals(0, lock.getReadLockCount());
    }

    //endregion

    //region ==================== Try Lock Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testTryToAcquireWriteLock() throws Exception {
        // Test successful try
        assertTrue(lock.tryToAcquireWriteLock());
        assertEquals(Thread.currentThread(), lock.getWriteLockThread());
        lock.releaseWriteLock();

        // Test failed try when lock is held
        CountDownLatch lockAcquired = new CountDownLatch(1);
        Future<?> holder = executor.submit(() -> {
            try {
                lock.acquireWriteLock(5);
                lockAcquired.countDown();
                Thread.sleep(500);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        lockAcquired.await();
        assertFalse(lock.tryToAcquireWriteLock()); // Even with higher priority

        holder.get(2, TimeUnit.SECONDS);
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testTryToAcquireWriteLockReentrant() throws InterruptedException {
        lock.acquireWriteLock(5);
        assertTrue(lock.tryToAcquireWriteLock());
        assertEquals(2, lock.getWriteLockCount());
        lock.releaseWriteLock();
        lock.releaseWriteLock();
    }

    //endregion

    //region ==================== Exclusivity Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testWriteLockExcludesReaders() throws Exception {
        CountDownLatch writerAcquired = new CountDownLatch(1);
        CountDownLatch readerAttempted = new CountDownLatch(1);
        AtomicBoolean readerBlocked = new AtomicBoolean(true);

        // Writer holds lock
        Future<?> writer = executor.submit(() -> {
            try {
                lock.acquireWriteLock(5);
                writerAcquired.countDown();
                Thread.sleep(300);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        writerAcquired.await();

        // Reader tries to acquire
        Future<?> reader = executor.submit(() -> {
            try {
                readerAttempted.countDown();
                boolean acquired = lock.acquireReadLock(10, 100, TimeUnit.MILLISECONDS);
                readerBlocked.set(!acquired);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        readerAttempted.await();
        reader.get(2, TimeUnit.SECONDS);
        writer.get(2, TimeUnit.SECONDS);

        assertTrue(readerBlocked.get());
    }

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testReadersExcludeWriter() throws Exception {
        int numReaders = 3;
        CountDownLatch readersAcquired = new CountDownLatch(numReaders);
        CountDownLatch writerAttempted = new CountDownLatch(1);
        AtomicBoolean writerBlocked = new AtomicBoolean(true);

        // Multiple readers hold locks
        List<Future<?>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(executor.submit(() -> {
                try {
                    lock.acquireReadLock(5);
                    readersAcquired.countDown();
                    Thread.sleep(300);
                    lock.releaseReadLock();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        readersAcquired.await();

        // Writer tries to acquire
        Future<?> writer = executor.submit(() -> {
            try {
                writerAttempted.countDown();
                boolean acquired = lock.acquireWriteLock(10, 100, TimeUnit.MILLISECONDS);
                writerBlocked.set(!acquired);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        writerAttempted.await();
        writer.get(2, TimeUnit.SECONDS);

        assertTrue(writerBlocked.get());

        for (Future<?> reader : readers) {
            reader.get(2, TimeUnit.SECONDS);
        }
    }

    //endregion

    //region ==================== Error Handling Tests ====================

    @Test
    void testReleaseWriteLockWithoutHolding() {
        assertThrows(RuntimeException.class, () -> lock.releaseWriteLock());
    }

    @Test
    void testReleaseWriteLockFromWrongThread() throws Exception {
        CountDownLatch lockAcquired = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                lock.acquireWriteLock(5);
                lockAcquired.countDown();
                Thread.sleep(100);
                lock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        lockAcquired.await();
        assertThrows(RuntimeException.class, () -> lock.releaseWriteLock());
    }

    @Test
    void testReleaseReadLockWithoutHolding() {
        assertThrows(RuntimeException.class, () -> lock.releaseReadLock());
    }

    //endregion

    //region ==================== Stress Tests ====================

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void stressTestMixedOperations() throws Exception {
        int numThreads = 20;
        int operationsPerThread = 100;
        AtomicInteger successfulOps = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numThreads);

        Random random = new Random();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    for (int op = 0; op < operationsPerThread; op++) {
                        int priority = random.nextInt(10) + 1;
                        boolean isWrite = random.nextBoolean();

                        if (isWrite) {
                            if (random.nextBoolean()) {
                                // Try with timeout
                                if (lock.acquireWriteLock(priority, 50, TimeUnit.MILLISECONDS)) {
                                    Thread.sleep(random.nextInt(5));
                                    lock.releaseWriteLock();
                                    successfulOps.incrementAndGet();
                                }
                            } else {
                                // Try without blocking
                                if (lock.tryToAcquireWriteLock()) {
                                    Thread.sleep(random.nextInt(5));
                                    lock.releaseWriteLock();
                                    successfulOps.incrementAndGet();
                                }
                            }
                        } else {
                            // Read operation
                            if (lock.acquireReadLock(priority, 50, TimeUnit.MILLISECONDS)) {
                                Thread.sleep(random.nextInt(5));
                                lock.releaseReadLock();
                                successfulOps.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    logger.error("Thread {} error: {}", threadId, e.getMessage());
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        assertEquals(0, errors.get());
        assertTrue(successfulOps.get() > 0);

        // Verify lock is in clean state
        assertEquals(0, lock.getReadLockCount());
        assertEquals(0, lock.getWriteLockCount());
        assertNull(lock.getWriteLockThread());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void stressTestHighContention() throws Exception {
        int numWriters = 10;
        int numReaders = 10;
        AtomicInteger writeCompletions = new AtomicInteger(0);
        AtomicInteger readCompletions = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numWriters + numReaders);

        // Create writers with varying priorities
        for (int i = 0; i < numWriters; i++) {
            final int priority = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 10; j++) {
                        lock.acquireWriteLock(priority);
                        Thread.sleep(10);
                        lock.releaseWriteLock();
                        writeCompletions.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // Create readers with varying priorities
        for (int i = 0; i < numReaders; i++) {
            final int priority = i + 1;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 20; j++) {
                        lock.acquireReadLock(priority);
                        Thread.sleep(5);
                        lock.releaseReadLock();
                        readCompletions.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(10, TimeUnit.SECONDS));

        assertEquals(numWriters * 10, writeCompletions.get());
        assertEquals(numReaders * 20, readCompletions.get());

        // Verify clean state
        assertEquals(0, lock.getReadLockCount());
        assertEquals(0, lock.getWriteLockCount());
    }

    //endregion

    //region ==================== Performance Tests ====================

    @Test
    @Timeout(value = TEST_TIMEOUT_MS, unit = TimeUnit.MILLISECONDS)
    void testUnhealthyWriteLockAcquireTimeLogging() throws Exception {
        // Create lock with very short unhealthy threshold
        PWRReentrantReadWriteLock testLock = new PWRReentrantReadWriteLock("perfTestLock", 10);

        CountDownLatch holderReady = new CountDownLatch(1);

        // Hold lock to cause delay
        Future<?> holder = executor.submit(() -> {
            try {
                testLock.acquireWriteLock(5);
                holderReady.countDown();
                Thread.sleep(50); // Hold for more than unhealthy threshold
                testLock.releaseWriteLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        holderReady.await();

        // This should trigger unhealthy log
        testLock.acquireWriteLock(10);
        testLock.releaseWriteLock();

        holder.get(2, TimeUnit.SECONDS);
    }

    //endregion
}
