package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.Hex;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * Performance tests for the MerkleTree implementation.
 * Tests focus on:
 * 1. Measuring operation times for different tree sizes
 * 2. Benchmarking critical operations
 * 3. Identifying performance bottlenecks
 */
public class MerkleTreePerformanceTest {
    private static final int VERY_LARGE_TREE_SIZE = 10000;
    private static final int LARGE_TREE_SIZE = 1000;
    private static final int MEDIUM_TREE_SIZE = 100;
    
    private static final int WARMUP_ITERATIONS = 3;
    private static final int BENCHMARK_ITERATIONS = 10;
    
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        try {
            System.out.println("Starting MerkleTree Performance Tests...");
            deleteOldMerkleTrees();
            
            // Run performance tests
            benchmarkTreeCreation();
            benchmarkLeafAddition();
            benchmarkLeafUpdate();
            benchmarkTreeMerge();
            benchmarkDataOperations();
            
            System.out.println("All performance tests completed successfully!");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Performance tests failed: " + e.getMessage());
        }
    }
    
    /**
     * Benchmark tree creation with different sizes
     */
    private static void benchmarkTreeCreation() throws RocksDBException {
        System.out.println("\n=== Benchmarking Tree Creation ===");
        
        // Test different tree sizes
        int[] treeSizes = {10, 100, 1000, 10000};
        
        for (int size : treeSizes) {
            System.out.println("\nCreating tree with " + size + " leaves:");
            
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                MerkleTree tree = new MerkleTree("perfCreation_warmup_" + i);
                try {
                    for (int j = 0; j < Math.min(size, 100); j++) {
                        byte[] leafHash = createRandomHash();
                        tree.addLeaf(tree.new Node(leafHash));
                    }
                } finally {
                    tree.close();
                }
            }
            
            // Benchmark
            long[] times = new long[BENCHMARK_ITERATIONS];
            for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                MerkleTree tree = new MerkleTree("perfCreation_" + size + "_" + i);
                try {
                    long startTime = System.nanoTime();
                    
                    for (int j = 0; j < size; j++) {
                        byte[] leafHash = createRandomHash();
                        tree.addLeaf(tree.new Node(leafHash));
                    }
                    
                    long endTime = System.nanoTime();
                    times[i] = endTime - startTime;
                } finally {
                    tree.close();
                }
            }
            
            // Calculate statistics
            double avgTimeMs = calculateAverage(times) / 1_000_000.0;
            double stdDevMs = calculateStdDev(times) / 1_000_000.0;
            double opsPerSecond = size / (avgTimeMs / 1000.0);
            
            System.out.println("Average time: " + String.format("%.2f", avgTimeMs) + " ms");
            System.out.println("Standard deviation: " + String.format("%.2f", stdDevMs) + " ms");
            System.out.println("Operations per second: " + String.format("%.2f", opsPerSecond));
            System.out.println("Time per leaf: " + String.format("%.3f", avgTimeMs / size) + " ms");
        }
    }
    
    /**
     * Benchmark leaf addition to existing trees
     */
    private static void benchmarkLeafAddition() throws RocksDBException {
        System.out.println("\n=== Benchmarking Leaf Addition ===");
        
        // Test adding leaves to trees of different sizes
        int[] existingTreeSizes = {0, 10, 100, 1000};
        int leavesToAdd = 100;
        
        for (int existingSize : existingTreeSizes) {
            System.out.println("\nAdding " + leavesToAdd + " leaves to tree with " + existingSize + " existing leaves:");
            
            // Create tree with existing leaves
            MerkleTree tree = new MerkleTree("perfAddition_" + existingSize);
            try {
                for (int i = 0; i < existingSize; i++) {
                    byte[] leafHash = createRandomHash();
                    tree.addLeaf(tree.new Node(leafHash));
                }
                tree.flushToDisk();
                
                // Warmup
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    for (int j = 0; j < Math.min(10, leavesToAdd); j++) {
                        byte[] leafHash = createRandomHash();
                        tree.addLeaf(tree.new Node(leafHash));
                    }
                    tree.revertUnsavedChanges();
                }
                
                // Benchmark
                long[] times = new long[BENCHMARK_ITERATIONS];
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    long startTime = System.nanoTime();
                    
                    for (int j = 0; j < leavesToAdd; j++) {
                        byte[] leafHash = createRandomHash();
                        tree.addLeaf(tree.new Node(leafHash));
                    }
                    
                    long endTime = System.nanoTime();
                    times[i] = endTime - startTime;
                    
                    // Revert changes for next iteration
                    tree.revertUnsavedChanges();
                }
                
                // Calculate statistics
                double avgTimeMs = calculateAverage(times) / 1_000_000.0;
                double stdDevMs = calculateStdDev(times) / 1_000_000.0;
                double opsPerSecond = leavesToAdd / (avgTimeMs / 1000.0);
                
                System.out.println("Average time: " + String.format("%.2f", avgTimeMs) + " ms");
                System.out.println("Standard deviation: " + String.format("%.2f", stdDevMs) + " ms");
                System.out.println("Operations per second: " + String.format("%.2f", opsPerSecond));
                System.out.println("Time per leaf: " + String.format("%.3f", avgTimeMs / leavesToAdd) + " ms");
            } finally {
                tree.close();
            }
        }
    }
    
    /**
     * Benchmark leaf updates in trees of different sizes
     */
    private static void benchmarkLeafUpdate() throws RocksDBException {
        System.out.println("\n=== Benchmarking Leaf Updates ===");
        
        // Test updating leaves in trees of different sizes
        int[] treeSizes = {10, 100, 1000};
        int updatesToPerform = 50;
        
        for (int size : treeSizes) {
            System.out.println("\nUpdating leaves in tree with " + size + " leaves:");
            
            // Create tree with leaves
            MerkleTree tree = new MerkleTree("perfUpdate_" + size);
            try {
                // Store the actual leaf nodes for accurate updates
                Map<Integer, byte[]> leafHashes = new HashMap<>();
                
                // Add leaves to the tree and store their hashes
                for (int i = 0; i < size; i++) {
                    byte[] leafHash = createRandomHash();
                    tree.addLeaf(tree.new Node(leafHash));
                    leafHashes.put(i, leafHash);
                }
                tree.flushToDisk();
                
                // Limit updates to avoid running out of leaves
                int actualUpdatesToPerform = Math.min(updatesToPerform, size / 2);
                
                // Warmup with a small number of updates
                try {
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        for (int j = 0; j < Math.min(3, actualUpdatesToPerform); j++) {
                            // Get a random leaf index
                            int leafIndex = random.nextInt(leafHashes.size());
                            byte[] oldHash = leafHashes.get(leafIndex);
                            byte[] newHash = createRandomHash();
                            
                            // Only update if the old hash exists
                            if (oldHash != null) {
                                tree.updateLeaf(oldHash, newHash);
                                leafHashes.put(leafIndex, newHash);
                            }
                        }
                        // Revert changes after each warmup iteration
                        tree.revertUnsavedChanges();
                    }
                } catch (Exception e) {
                    System.out.println("Warning: Exception during warmup: " + e.getMessage());
                    // Continue with the benchmark despite warmup issues
                }
                
                // Refresh the tree to ensure we have a clean state
                tree.close();
                tree = new MerkleTree("perfUpdate_" + size);
                
                // Re-add all leaves
                leafHashes.clear();
                for (int i = 0; i < size; i++) {
                    byte[] leafHash = createRandomHash();
                    tree.addLeaf(tree.new Node(leafHash));
                    leafHashes.put(i, leafHash);
                }
                tree.flushToDisk();
                
                // Benchmark
                long[] times = new long[BENCHMARK_ITERATIONS];
                int successfulUpdates = 0;
                
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    // Create a copy of leaf hashes for this iteration
                    Map<Integer, byte[]> iterationLeafHashes = new HashMap<>(leafHashes);
                    List<Integer> availableIndices = new ArrayList<>(iterationLeafHashes.keySet());
                    
                    // Track successful updates in this iteration
                    int iterationSuccesses = 0;
                    
                    long startTime = System.nanoTime();
                    
                    for (int j = 0; j < actualUpdatesToPerform && !availableIndices.isEmpty(); j++) {
                        // Get a random index from available indices
                        int randomPosition = random.nextInt(availableIndices.size());
                        int leafIndex = availableIndices.get(randomPosition);
                        
                        byte[] oldHash = iterationLeafHashes.get(leafIndex);
                        byte[] newHash = createRandomHash();
                        
                        try {
                            tree.updateLeaf(oldHash, newHash);
                            iterationLeafHashes.put(leafIndex, newHash);
                            iterationSuccesses++;
                            
                            // Remove this index to avoid updating the same leaf twice
                            availableIndices.remove(randomPosition);
                        } catch (Exception e) {
                            // Skip this update if it fails
                            System.out.println("Warning: Update failed: " + e.getMessage());
                        }
                    }
                    
                    long endTime = System.nanoTime();
                    times[i] = endTime - startTime;
                    successfulUpdates += iterationSuccesses;
                    
                    // Revert changes for next iteration
                    tree.revertUnsavedChanges();
                }
                
                // Calculate statistics based on actual successful updates
                double avgTimeMs = calculateAverage(times) / 1_000_000.0;
                double stdDevMs = calculateStdDev(times) / 1_000_000.0;
                double avgSuccessfulUpdates = (double) successfulUpdates / BENCHMARK_ITERATIONS;
                double opsPerSecond = avgSuccessfulUpdates / (avgTimeMs / 1000.0);
                
                System.out.println("Average time: " + String.format("%.2f", avgTimeMs) + " ms");
                System.out.println("Standard deviation: " + String.format("%.2f", stdDevMs) + " ms");
                System.out.println("Average successful updates per iteration: " + 
                        String.format("%.1f", avgSuccessfulUpdates));
                System.out.println("Operations per second: " + String.format("%.2f", opsPerSecond));
                System.out.println("Time per update: " + 
                        String.format("%.3f", avgSuccessfulUpdates > 0 ? avgTimeMs / avgSuccessfulUpdates : 0) + " ms");
            } finally {
                tree.close();
            }
        }
    }
    
    /**
     * Benchmark tree merging with different sizes
     */
    private static void benchmarkTreeMerge() throws RocksDBException {
        System.out.println("\n=== Benchmarking Tree Merge ===");
        
        // Test merging trees of different sizes
        int[][] sizePairs = {{10, 10}, {100, 100}, {1000, 1000}, {100, 1000}};
        
        for (int[] pair : sizePairs) {
            int sourceSize = pair[0];
            int targetSize = pair[1];
            
            System.out.println("\nMerging source tree (" + sourceSize + " leaves) into target tree (" + targetSize + " leaves):");
            
            // Create source tree
            MerkleTree sourceTree = new MerkleTree("perfMergeSource_" + sourceSize);
            try {
                for (int i = 0; i < sourceSize; i++) {
                    byte[] leafHash = createRandomHash();
                    sourceTree.addLeaf(sourceTree.new Node(leafHash));
                }
                sourceTree.flushToDisk();
                
                // Warmup with smaller trees
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    MerkleTree warmupTarget = new MerkleTree("perfMergeWarmup_" + i);
                    try {
                        for (int j = 0; j < Math.min(10, targetSize); j++) {
                            byte[] leafHash = createRandomHash();
                            warmupTarget.addLeaf(warmupTarget.new Node(leafHash));
                        }
                        warmupTarget.flushToDisk();
                        warmupTarget.updateWithTree(sourceTree);
                    } finally {
                        warmupTarget.close();
                    }
                }
                
                // Benchmark
                long[] times = new long[BENCHMARK_ITERATIONS];
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    MerkleTree targetTree = new MerkleTree("perfMergeTarget_" + i);
                    try {
                        for (int j = 0; j < targetSize; j++) {
                            byte[] leafHash = createRandomHash();
                            targetTree.addLeaf(targetTree.new Node(leafHash));
                        }
                        targetTree.flushToDisk();
                        
                        long startTime = System.nanoTime();
                        targetTree.updateWithTree(sourceTree);
                        long endTime = System.nanoTime();
                        
                        times[i] = endTime - startTime;
                    } finally {
                        targetTree.close();
                    }
                }
                
                // Calculate statistics
                double avgTimeMs = calculateAverage(times) / 1_000_000.0;
                double stdDevMs = calculateStdDev(times) / 1_000_000.0;
                
                System.out.println("Average time: " + String.format("%.2f", avgTimeMs) + " ms");
                System.out.println("Standard deviation: " + String.format("%.2f", stdDevMs) + " ms");
                System.out.println("Time per source leaf: " + String.format("%.3f", avgTimeMs / sourceSize) + " ms");
            } finally {
                sourceTree.close();
            }
        }
    }
    
    /**
     * Benchmark key-value data operations
     */
    private static void benchmarkDataOperations() throws RocksDBException {
        System.out.println("\n=== Benchmarking Data Operations ===");
        
        // Test data operations with different numbers of key-value pairs
        int[] dataSizes = {10, 100, 1000};
        
        for (int size : dataSizes) {
            System.out.println("\nPerforming data operations with " + size + " key-value pairs:");
            
            MerkleTree tree = new MerkleTree("perfData_" + size);
            try {
                // Generate random keys and values
                List<byte[]> keys = new ArrayList<>();
                List<byte[]> values = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    keys.add(("key" + i).getBytes());
                    values.add(createRandomHash());
                }
                
                // First add all data once to ensure the tree is properly initialized
                for (int j = 0; j < size; j++) {
                    try {
                        tree.addOrUpdateData(keys.get(j), values.get(j));
                    } catch (Exception e) {
                        // Ignore exceptions during initialization
                    }
                }
                tree.flushToDisk();
                
                // Create a fresh tree for benchmarking
                tree.close();
                tree = new MerkleTree("perfData_" + size);
                
                // Benchmark addOrUpdateData
                System.out.println("Testing addOrUpdateData:");
                long[] addTimes = new long[BENCHMARK_ITERATIONS];
                int successfulOperations = 0;
                
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    int iterationSuccesses = 0;
                    
                    long startTime = System.nanoTime();
                    
                    for (int j = 0; j < size; j++) {
                        try {
                            tree.addOrUpdateData(keys.get(j), values.get(j));
                            iterationSuccesses++;
                        } catch (Exception e) {
                            // Skip failed operations
                            System.out.println("Warning: Operation failed: " + e.getMessage());
                        }
                    }
                    
                    long endTime = System.nanoTime();
                    addTimes[i] = endTime - startTime;
                    successfulOperations += iterationSuccesses;
                    
                    // Flush for next iteration
                    tree.flushToDisk();
                }
                
                double avgSuccessfulOps = (double) successfulOperations / BENCHMARK_ITERATIONS;
                double avgAddTimeMs = calculateAverage(addTimes) / 1_000_000.0;
                double stdDevAddMs = calculateStdDev(addTimes) / 1_000_000.0;
                double addOpsPerSecond = avgSuccessfulOps > 0 ? avgSuccessfulOps / (avgAddTimeMs / 1000.0) : 0;
                
                System.out.println("Average time: " + String.format("%.2f", avgAddTimeMs) + " ms");
                System.out.println("Standard deviation: " + String.format("%.2f", stdDevAddMs) + " ms");
                System.out.println("Average successful operations per iteration: " + 
                        String.format("%.1f", avgSuccessfulOps));
                System.out.println("Operations per second: " + String.format("%.2f", addOpsPerSecond));
                System.out.println("Time per operation: " + 
                        String.format("%.3f", avgSuccessfulOps > 0 ? avgAddTimeMs / avgSuccessfulOps : 0) + " ms");
                
                // Benchmark getData
                System.out.println("\nTesting getData:");
                long[] getTimes = new long[BENCHMARK_ITERATIONS];
                successfulOperations = 0;
                
                for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
                    int iterationSuccesses = 0;
                    
                    long startTime = System.nanoTime();
                    
                    for (int j = 0; j < size; j++) {
                        try {
                            byte[] data = tree.getData(keys.get(j));
                            if (data != null) {
                                iterationSuccesses++;
                            }
                        } catch (Exception e) {
                            // Skip failed operations
                        }
                    }
                    
                    long endTime = System.nanoTime();
                    getTimes[i] = endTime - startTime;
                    successfulOperations += iterationSuccesses;
                }
                
                avgSuccessfulOps = (double) successfulOperations / BENCHMARK_ITERATIONS;
                double avgGetTimeMs = calculateAverage(getTimes) / 1_000_000.0;
                double stdDevGetMs = calculateStdDev(getTimes) / 1_000_000.0;
                double getOpsPerSecond = avgSuccessfulOps > 0 ? avgSuccessfulOps / (avgGetTimeMs / 1000.0) : 0;
                
                System.out.println("Average time: " + String.format("%.2f", avgGetTimeMs) + " ms");
                System.out.println("Standard deviation: " + String.format("%.2f", stdDevGetMs) + " ms");
                System.out.println("Average successful operations per iteration: " + 
                        String.format("%.1f", avgSuccessfulOps));
                System.out.println("Operations per second: " + String.format("%.2f", getOpsPerSecond));
                System.out.println("Time per operation: " + 
                        String.format("%.3f", avgSuccessfulOps > 0 ? avgGetTimeMs / avgSuccessfulOps : 0) + " ms");
            } finally {
                tree.close();
            }
        }
    }
    
    // Helper methods
    private static byte[] createRandomHash() {
        byte[] hash = new byte[32];
        random.nextBytes(hash);
        return hash;
    }
    
    private static double calculateAverage(long[] values) {
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return (double) sum / values.length;
    }
    
    private static double calculateStdDev(long[] values) {
        double avg = calculateAverage(values);
        double sumSquaredDiff = 0;
        for (long value : values) {
            double diff = value - avg;
            sumSquaredDiff += diff * diff;
        }
        return Math.sqrt(sumSquaredDiff / values.length);
    }
    
    private static void deleteOldMerkleTrees() {
        String[] treeNames = {
                "perfCreation_warmup_", "perfCreation_", 
                "perfAddition_", "perfUpdate_",
                "perfMergeSource_", "perfMergeTarget_", "perfMergeWarmup_",
                "perfData_"
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
