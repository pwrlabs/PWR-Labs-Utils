package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.Hex;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive stress tests for the MerkleTree implementation.
 * Tests focus on:
 * 1. Large tree operations (thousands of nodes)
 * 2. Concurrent modifications under heavy load
 * 3. Memory usage and performance benchmarks
 * 4. Edge cases and error handling under stress
 */
public class MerkleTreeStressTest {
    private static final int VERY_LARGE_TREE_SIZE = 10000;
    private static final int LARGE_TREE_SIZE = 1000;
    private static final int MEDIUM_TREE_SIZE = 100;
    
    private static final int MAX_THREADS = 20;
    private static final int OPERATIONS_PER_THREAD = 50;
    
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        try {
            System.out.println("Starting MerkleTree Stress Tests...");
            deleteOldMerkleTrees();
            
            // Run stress tests
            testLargeTreeCreation();
            testConcurrentAddLeaves();
            testConcurrentUpdateLeaves();
            testMixedConcurrentOperations();
            testLargeTreeMerge();
            testTreeResilience();
            
            System.out.println("All stress tests completed successfully!");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Stress tests failed: " + e.getMessage());
        }
    }
    
    /**
     * Test creating a very large tree with thousands of leaves
     */
    private static void testLargeTreeCreation() throws RocksDBException {
        System.out.println("\n=== Testing Large Tree Creation ===");
        long startTime = System.currentTimeMillis();
        
        MerkleTree tree = new MerkleTree("stressLargeTree");
        try {
            System.out.println("Creating tree with " + VERY_LARGE_TREE_SIZE + " leaves...");
            
            for (int i = 0; i < VERY_LARGE_TREE_SIZE; i++) {
                byte[] leafHash = createRandomHash();
                tree.addLeaf(tree.new Node(leafHash));
                
                if (i > 0 && i % 1000 == 0) {
                    System.out.println("Added " + i + " leaves...");
                }
            }
            
            tree.flushToDisk();
            long endTime = System.currentTimeMillis();
            
            System.out.println("Successfully created tree with " + tree.getNumLeaves() + " leaves");
            System.out.println("Tree depth: " + tree.getDepth());
            System.out.println("Time taken: " + (endTime - startTime) + "ms");
            
            // Verify tree integrity
            assertNotNull(tree.getRootHash(), "Root hash should not be null");
            assertEquals(tree.getNumLeaves(), VERY_LARGE_TREE_SIZE, "Tree should have correct number of leaves");
        } finally {
            tree.close();
        }
    }
    
    /**
     * Test concurrent addition of leaves by multiple threads
     */
    private static void testConcurrentAddLeaves() throws Exception {
        System.out.println("\n=== Testing Concurrent Add Leaves ===");
        
        MerkleTree tree = new MerkleTree("stressConcurrentAdd");
        try {
            final int numThreads = MAX_THREADS;
            final int operationsPerThread = OPERATIONS_PER_THREAD;
            final CountDownLatch latch = new CountDownLatch(numThreads);
            final AtomicInteger successCount = new AtomicInteger(0);
            final ConcurrentHashMap<String, byte[]> addedHashes = new ConcurrentHashMap<>();
            
            System.out.println("Starting " + numThreads + " threads, each adding " + 
                    operationsPerThread + " leaves...");
            
            long startTime = System.currentTimeMillis();
            
            // Create and start threads
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            byte[] hash = createRandomHash();
                            tree.addLeafIfMissing(hash);
                            addedHashes.put(Hex.toHexString(hash), hash);
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Exception in thread " + threadId + ": " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
                threads[t].start();
            }
            
            // Wait for all threads to complete
            latch.await();
            long endTime = System.currentTimeMillis();
            
            // Verify results
            tree.flushToDisk();
            System.out.println("All threads completed. Successful operations: " + successCount.get());
            System.out.println("Tree leaves: " + tree.getNumLeaves());
            System.out.println("Unique hashes added: " + addedHashes.size());
            System.out.println("Time taken: " + (endTime - startTime) + "ms");
            
            // There might be some duplicate hashes due to random generation
            assertTrue(tree.getNumLeaves() <= addedHashes.size(), 
                    "Tree should not have more leaves than unique hashes added");
            assertTrue(tree.getNumLeaves() > 0, "Tree should have leaves");
        } finally {
            tree.close();
        }
    }
    
    /**
     * Test concurrent updates to existing leaves
     */
    private static void testConcurrentUpdateLeaves() throws Exception {
        System.out.println("\n=== Testing Concurrent Update Leaves ===");
        
        MerkleTree tree = new MerkleTree("stressConcurrentUpdate");
        try {
            // First create a tree with leaves
            final int initialLeaves = MEDIUM_TREE_SIZE;
            final List<byte[]> leafHashes = new ArrayList<>();
            
            System.out.println("Creating initial tree with " + initialLeaves + " leaves...");
            for (int i = 0; i < initialLeaves; i++) {
                byte[] hash = createRandomHash();
                leafHashes.add(hash);
                tree.addLeaf(tree.new Node(hash));
            }
            tree.flushToDisk();
            
            // Now update leaves concurrently
            final int numThreads = Math.min(MAX_THREADS, initialLeaves);
            final CountDownLatch latch = new CountDownLatch(numThreads);
            final AtomicInteger successCount = new AtomicInteger(0);
            
            System.out.println("Starting " + numThreads + " threads to update leaves...");
            long startTime = System.currentTimeMillis();
            
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        // Each thread updates a subset of leaves
                        int leavesPerThread = initialLeaves / numThreads;
                        int startIdx = threadId * leavesPerThread;
                        int endIdx = (threadId == numThreads - 1) ? 
                                initialLeaves : (threadId + 1) * leavesPerThread;
                        
                        for (int i = startIdx; i < endIdx; i++) {
                            byte[] oldHash = leafHashes.get(i);
                            byte[] newHash = createRandomHash();
                            tree.updateLeaf(oldHash, newHash);
                            leafHashes.set(i, newHash);
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Exception in thread " + threadId + ": " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
                threads[t].start();
            }
            
            // Wait for all threads to complete
            latch.await();
            long endTime = System.currentTimeMillis();
            
            // Verify results
            tree.flushToDisk();
            System.out.println("All threads completed. Successful updates: " + successCount.get());
            System.out.println("Tree leaves: " + tree.getNumLeaves());
            System.out.println("Time taken: " + (endTime - startTime) + "ms");
            
            assertEquals(tree.getNumLeaves(), initialLeaves, 
                    "Tree should maintain the same number of leaves after updates");
        } finally {
            tree.close();
        }
    }
    
    /**
     * Test mixed concurrent operations (add, update, get)
     */
    private static void testMixedConcurrentOperations() throws Exception {
        System.out.println("\n=== Testing Mixed Concurrent Operations ===");
        
        MerkleTree tree = new MerkleTree("stressMixedOperations");
        try {
            // First create a tree with some initial leaves
            final int initialLeaves = MEDIUM_TREE_SIZE;
            final ConcurrentHashMap<String, byte[]> leafHashes = new ConcurrentHashMap<>();
            
            System.out.println("Creating initial tree with " + initialLeaves + " leaves...");
            for (int i = 0; i < initialLeaves; i++) {
                byte[] hash = createRandomHash();
                String hexHash = Hex.toHexString(hash);
                leafHashes.put(hexHash, hash);
                tree.addLeaf(tree.new Node(hash));
            }
            tree.flushToDisk();
            
            // Now perform mixed operations concurrently
            final int numThreads = MAX_THREADS;
            final int operationsPerThread = OPERATIONS_PER_THREAD;
            final CountDownLatch latch = new CountDownLatch(numThreads);
            final AtomicInteger addCount = new AtomicInteger(0);
            final AtomicInteger updateCount = new AtomicInteger(0);
            final AtomicInteger errorCount = new AtomicInteger(0);
            
            System.out.println("Starting " + numThreads + " threads for mixed operations...");
            long startTime = System.currentTimeMillis();
            
            Thread[] threads = new Thread[numThreads];
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        for (int i = 0; i < operationsPerThread; i++) {
                            // Randomly choose operation: 0=add, 1=update
                            int operation = random.nextInt(2);
                            
                            if (operation == 0) {
                                // Add new leaf
                                byte[] hash = createRandomHash();
                                String hexHash = Hex.toHexString(hash);
                                tree.addLeafIfMissing(hash);
                                leafHashes.put(hexHash, hash);
                                addCount.incrementAndGet();
                            } else {
                                // Update existing leaf if we have any
                                if (!leafHashes.isEmpty()) {
                                    try {
                                        // Get a random existing hash
                                        List<String> keys = new ArrayList<>(leafHashes.keySet());
                                        String randomKey = keys.get(random.nextInt(keys.size()));
                                        byte[] oldHash = leafHashes.get(randomKey);
                                        
                                        // Update it
                                        byte[] newHash = createRandomHash();
                                        String newHexHash = Hex.toHexString(newHash);
                                        tree.updateLeaf(oldHash, newHash);
                                        
                                        // Update our tracking map
                                        leafHashes.remove(randomKey);
                                        leafHashes.put(newHexHash, newHash);
                                        updateCount.incrementAndGet();
                                    } catch (Exception e) {
                                        // This can happen if multiple threads try to update the same leaf
                                        errorCount.incrementAndGet();
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.err.println("Exception in thread " + threadId + ": " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
                threads[t].start();
            }
            
            // Wait for all threads to complete
            latch.await();
            long endTime = System.currentTimeMillis();
            
            // Verify results
            tree.flushToDisk();
            System.out.println("All threads completed.");
            System.out.println("Add operations: " + addCount.get());
            System.out.println("Update operations: " + updateCount.get());
            System.out.println("Expected errors: " + errorCount.get());
            System.out.println("Tree leaves: " + tree.getNumLeaves());
            System.out.println("Time taken: " + (endTime - startTime) + "ms");
            
            assertTrue(tree.getNumLeaves() >= initialLeaves, 
                    "Tree should have at least the initial number of leaves");
        } finally {
            tree.close();
        }
    }
    
    /**
     * Test merging two large trees
     */
    private static void testLargeTreeMerge() throws RocksDBException {
        System.out.println("\n=== Testing Large Tree Merge ===");
        
        // Create source tree
        MerkleTree sourceTree = new MerkleTree("stressLargeSourceTree");
        try {
            System.out.println("Creating source tree with " + LARGE_TREE_SIZE + " leaves...");
            for (int i = 0; i < LARGE_TREE_SIZE; i++) {
                byte[] hash = createRandomHash();
                sourceTree.addLeaf(sourceTree.new Node(hash));
            }
            sourceTree.flushToDisk();
            
            // Create target tree with different leaves
            MerkleTree targetTree = new MerkleTree("stressLargeTargetTree");
            try {
                System.out.println("Creating target tree with " + LARGE_TREE_SIZE + " leaves...");
                for (int i = 0; i < LARGE_TREE_SIZE; i++) {
                    byte[] hash = createRandomHash();
                    targetTree.addLeaf(targetTree.new Node(hash));
                }
                targetTree.flushToDisk();
                
                // Save target state before merge
                byte[] targetRootBeforeMerge = targetTree.getRootHash();
                int targetLeavesBeforeMerge = targetTree.getNumLeaves();
                
                // Perform merge
                System.out.println("Merging trees...");
                long startTime = System.currentTimeMillis();
                targetTree.updateWithTree(sourceTree);
                long endTime = System.currentTimeMillis();
                
                // Verify results
                System.out.println("Merge completed in " + (endTime - startTime) + "ms");
                System.out.println("Target tree leaves after merge: " + targetTree.getNumLeaves());
                
                assertArrayEquals(sourceTree.getRootHash(), targetTree.getRootHash(),
                        "Target root should match source root after merge");
                assertEquals(targetTree.getNumLeaves(), sourceTree.getNumLeaves(),
                        "Target should have same number of leaves as source");
                assertNotEquals(targetRootBeforeMerge, targetTree.getRootHash(),
                        "Target root should change after merge");
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    /**
     * Test tree resilience under error conditions
     */
    private static void testTreeResilience() throws RocksDBException {
        System.out.println("\n=== Testing Tree Resilience ===");
        
        MerkleTree tree = new MerkleTree("stressResilience");
        try {
            // Add some initial leaves
            System.out.println("Creating initial tree...");
            List<byte[]> leafHashes = new ArrayList<>();
            for (int i = 0; i < MEDIUM_TREE_SIZE; i++) {
                byte[] hash = createRandomHash();
                leafHashes.add(hash);
                tree.addLeaf(tree.new Node(hash));
            }
            tree.flushToDisk();
            
            // Test 1: Try to update non-existent leaves
            System.out.println("Testing update of non-existent leaves...");
            int errorCount = 0;
            for (int i = 0; i < 100; i++) {
                try {
                    byte[] nonExistentHash = createRandomHash();
                    byte[] newHash = createRandomHash();
                    tree.updateLeaf(nonExistentHash, newHash);
                } catch (Exception e) {
                    errorCount++;
                }
            }
            System.out.println("Caught " + errorCount + " expected errors");
            
            // Test 2: Try to add null leaves
            System.out.println("Testing addition of null leaves...");
            errorCount = 0;
            for (int i = 0; i < 10; i++) {
                try {
                    tree.addLeafIfMissing(null);
                } catch (Exception e) {
                    errorCount++;
                }
            }
            System.out.println("Caught " + errorCount + " expected errors");
            
            // Test 3: Verify tree state is still consistent
            tree.flushToDisk();
            assertEquals(tree.getNumLeaves(), MEDIUM_TREE_SIZE,
                    "Tree should maintain correct leaf count after error tests");
            
            // Test 4: Verify we can still add leaves after errors
            System.out.println("Testing tree functionality after errors...");
            byte[] newHash = createRandomHash();
            tree.addLeaf(tree.new Node(newHash));
            assertEquals(tree.getNumLeaves(), MEDIUM_TREE_SIZE + 1,
                    "Tree should allow new leaves after error tests");
        } finally {
            tree.close();
        }
    }
    
    // Helper methods
    private static byte[] createRandomHash() {
        byte[] hash = new byte[32];
        random.nextBytes(hash);
        return hash;
    }
    
    private static void deleteOldMerkleTrees() {
        String[] treeNames = {
                "stressLargeTree", "stressConcurrentAdd", "stressConcurrentUpdate",
                "stressMixedOperations", "stressLargeSourceTree", "stressLargeTargetTree",
                "stressResilience"
        };
        
        System.out.println("Cleaning up old test trees...");
        for (String treeName : treeNames) {
            deleteTree(treeName);
        }
    }
    
    private static void deleteTree(String treeName) {
        try {
            File treeDir = new File("merkleTree/" + treeName);
            if (treeDir.exists() && treeDir.isDirectory()) {
                deleteDirectory(treeDir);
                System.out.println("Deleted tree: " + treeName);
            }
        } catch (Exception e) {
            System.out.println("Error deleting tree " + treeName + ": " + e.getMessage());
        }
    }
    
    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
    
    // Assertion methods
    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("Assertion failed: " + message);
        }
    }
    
    private static void assertEquals(int actual, int expected, String message) {
        if (actual != expected) {
            throw new AssertionError(message + " (Expected: " + expected + ", Actual: " + actual + ")");
        }
    }
    
    private static void assertArrayEquals(byte[] a, byte[] b, String message) {
        if (!Arrays.equals(a, b)) {
            throw new AssertionError("Array mismatch: " + message);
        }
    }
    
    private static void assertNotNull(Object obj, String message) {
        if (obj == null) {
            throw new AssertionError(message);
        }
    }
    
    private static void assertNotEquals(byte[] a, byte[] b, String message) {
        if (Arrays.equals(a, b)) {
            throw new AssertionError(message);
        }
    }
}
