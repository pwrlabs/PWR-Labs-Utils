package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.Hex;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;

/**
 * Memory usage tests for the MerkleTree implementation.
 * Tests focus on:
 * 1. Memory consumption during large tree operations
 * 2. Memory leaks during repeated operations
 * 3. Performance under memory pressure
 */
public class MerkleTreeMemoryTest {
    private static final int VERY_LARGE_TREE_SIZE = 50000;
    private static final int LARGE_TREE_SIZE = 10000;
    private static final int MEDIUM_TREE_SIZE = 1000;
    
    private static final int ITERATIONS = 5;
    private static final Random random = new Random();
    
    private static final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    
    public static void main(String[] args) {
        try {
            System.out.println("Starting MerkleTree Memory Tests...");
            deleteOldMerkleTrees();
            
            // Run memory tests
            testMemoryUsageLargeTree();
            testMemoryLeakRepeatedOperations();
            testPerformanceUnderMemoryPressure();
            
            System.out.println("All memory tests completed successfully!");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Memory tests failed: " + e.getMessage());
        }
    }
    
    /**
     * Test memory usage when creating and manipulating a large tree
     */
    private static void testMemoryUsageLargeTree() throws RocksDBException {
        System.out.println("\n=== Testing Memory Usage with Large Tree ===");
        
        // Measure baseline memory
        System.gc();
        MemoryUsage baselineMemory = memoryBean.getHeapMemoryUsage();
        System.out.println("Baseline memory usage: " + formatMemory(baselineMemory.getUsed()));
        
        MerkleTree tree = new MerkleTree("memoryLargeTree");
        try {
            // Create a large tree
            System.out.println("Creating tree with " + LARGE_TREE_SIZE + " leaves...");
            long startTime = System.currentTimeMillis();
            
            for (int i = 0; i < LARGE_TREE_SIZE; i++) {
                byte[] leafHash = createRandomHash();
                tree.addLeaf(tree.new Node(leafHash));
                
                if (i > 0 && i % 1000 == 0) {
                    System.out.println("Added " + i + " leaves...");
                    // Measure memory at intervals
                    MemoryUsage currentMemory = memoryBean.getHeapMemoryUsage();
                    System.out.println("Current memory usage: " + formatMemory(currentMemory.getUsed()));
                }
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("Tree creation completed in " + (endTime - startTime) + "ms");
            
            // Measure memory after tree creation
            System.gc();
            MemoryUsage afterCreationMemory = memoryBean.getHeapMemoryUsage();
            System.out.println("Memory after tree creation: " + formatMemory(afterCreationMemory.getUsed()));
            
            // Flush to disk and measure again
            System.out.println("Flushing tree to disk...");
            startTime = System.currentTimeMillis();
            tree.flushToDisk();
            endTime = System.currentTimeMillis();
            System.out.println("Flush completed in " + (endTime - startTime) + "ms");
            
            System.gc();
            MemoryUsage afterFlushMemory = memoryBean.getHeapMemoryUsage();
            System.out.println("Memory after flush: " + formatMemory(afterFlushMemory.getUsed()));
            
            // Verify tree state
            System.out.println("Tree has " + tree.getNumLeaves() + " leaves and depth " + tree.getDepth());
        } finally {
            tree.close();
        }
        
        // Measure memory after tree is closed
        System.gc();
        MemoryUsage finalMemory = memoryBean.getHeapMemoryUsage();
        System.out.println("Final memory usage: " + formatMemory(finalMemory.getUsed()));
        System.out.println("Memory difference from baseline: " + 
                formatMemory(finalMemory.getUsed() - baselineMemory.getUsed()));
    }
    
    /**
     * Test for memory leaks during repeated operations
     */
    private static void testMemoryLeakRepeatedOperations() throws RocksDBException {
        System.out.println("\n=== Testing for Memory Leaks with Repeated Operations ===");
        
        // Measure baseline memory
        System.gc();
        MemoryUsage baselineMemory = memoryBean.getHeapMemoryUsage();
        System.out.println("Baseline memory usage: " + formatMemory(baselineMemory.getUsed()));
        
        // Perform repeated operations
        System.out.println("Performing " + ITERATIONS + " iterations of tree operations...");
        
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            System.out.println("\nIteration " + (iteration + 1) + ":");
            
            MerkleTree tree = new MerkleTree("memoryLeakTest" + iteration);
            try {
                // Add leaves
                System.out.println("Adding " + MEDIUM_TREE_SIZE + " leaves...");
                for (int i = 0; i < MEDIUM_TREE_SIZE; i++) {
                    byte[] leafHash = createRandomHash();
                    tree.addLeaf(tree.new Node(leafHash));
                }
                
                // Update some leaves
                System.out.println("Updating leaves...");
                for (int i = 0; i < MEDIUM_TREE_SIZE / 10; i++) {
                    byte[] oldHash = createTestHash(i);
                    byte[] newHash = createRandomHash();
                    try {
                        tree.updateLeaf(oldHash, newHash);
                    } catch (Exception e) {
                        // Expected for some updates
                    }
                }
                
                // Flush and close
                tree.flushToDisk();
            } finally {
                tree.close();
            }
            
            // Measure memory after each iteration
            System.gc();
            MemoryUsage currentMemory = memoryBean.getHeapMemoryUsage();
            System.out.println("Memory after iteration " + (iteration + 1) + ": " + 
                    formatMemory(currentMemory.getUsed()));
        }
        
        // Measure final memory
        System.gc();
        MemoryUsage finalMemory = memoryBean.getHeapMemoryUsage();
        System.out.println("\nFinal memory usage: " + formatMemory(finalMemory.getUsed()));
        System.out.println("Memory difference from baseline: " + 
                formatMemory(finalMemory.getUsed() - baselineMemory.getUsed()));
        
        // If memory usage keeps increasing significantly across iterations, there might be a leak
        if (finalMemory.getUsed() > baselineMemory.getUsed() * 1.5) {
            System.out.println("WARNING: Possible memory leak detected. Memory usage increased significantly.");
        } else {
            System.out.println("No significant memory leak detected.");
        }
    }
    
    /**
     * Test performance under memory pressure
     */
    private static void testPerformanceUnderMemoryPressure() throws RocksDBException {
        System.out.println("\n=== Testing Performance Under Memory Pressure ===");
        
        // Create memory pressure by allocating a large array
        System.out.println("Creating memory pressure...");
        List<byte[]> memoryPressure = new ArrayList<>();
        try {
            // Allocate memory until we reach about 70% of max heap
            long maxHeap = Runtime.getRuntime().maxMemory();
            long targetUsage = (long)(maxHeap * 0.7);
            long allocated = 0;
            
            while (allocated < targetUsage) {
                byte[] block = new byte[1024 * 1024]; // 1MB blocks
                memoryPressure.add(block);
                allocated += block.length;
                
                if (memoryPressure.size() % 10 == 0) {
                    System.out.println("Allocated " + formatMemory(allocated) + " of memory");
                }
            }
            
            System.out.println("Created memory pressure: " + formatMemory(allocated));
            
            // Now perform tree operations under memory pressure
            System.out.println("Performing tree operations under memory pressure...");
            
            MerkleTree tree = new MerkleTree("memoryPressureTest");
            try {
                // Measure time to create tree under memory pressure
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < MEDIUM_TREE_SIZE; i++) {
                    byte[] leafHash = createRandomHash();
                    tree.addLeaf(tree.new Node(leafHash));
                }
                
                long endTime = System.currentTimeMillis();
                System.out.println("Tree creation under memory pressure: " + (endTime - startTime) + "ms");
                
                // Measure time to flush under memory pressure
                startTime = System.currentTimeMillis();
                tree.flushToDisk();
                endTime = System.currentTimeMillis();
                System.out.println("Flush under memory pressure: " + (endTime - startTime) + "ms");
                
                // Measure time to update leaves under memory pressure
                startTime = System.currentTimeMillis();
                int updateCount = 0;
                for (int i = 0; i < MEDIUM_TREE_SIZE / 5; i++) {
                    try {
                        byte[] oldHash = createTestHash(i);
                        byte[] newHash = createRandomHash();
                        tree.updateLeaf(oldHash, newHash);
                        updateCount++;
                    } catch (Exception e) {
                        // Expected for some updates
                    }
                }
                endTime = System.currentTimeMillis();
                System.out.println("Leaf updates under memory pressure: " + (endTime - startTime) + 
                        "ms for " + updateCount + " successful updates");
            } finally {
                tree.close();
            }
        } finally {
            // Release memory pressure
            memoryPressure.clear();
            System.gc();
        }
    }
    
    // Helper methods
    private static byte[] createRandomHash() {
        byte[] hash = new byte[32];
        random.nextBytes(hash);
        return hash;
    }
    
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }
    
    private static String formatMemory(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }
    
    private static void deleteOldMerkleTrees() {
        String[] treeNames = {
                "memoryLargeTree", "memoryLeakTest", "memoryPressureTest"
        };
        
        System.out.println("Cleaning up old test trees...");
        for (String treeName : treeNames) {
            File folder = new File("merkleTree");
            if (folder.exists()) {
                File[] files = folder.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isDirectory() && file.getName().startsWith(treeName)) {
                            deleteDirectory(file);
                        }
                    }
                }
            }
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
}
