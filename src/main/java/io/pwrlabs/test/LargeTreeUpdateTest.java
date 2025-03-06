package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import io.pwrlabs.hashing.PWRHash;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class LargeTreeUpdateTest {

    public static void main(String[] args) {
        try {
            deleteOldMerkleTrees();

            testLargeTreeUpdate();
            System.out.println("testLargeTreeUpdate passed");

            testDifferentSizeTreeUpdate();
            System.out.println("testDifferentSizeTreeUpdate passed");

            System.out.println("All tests passed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Tests failed: " + e.getMessage());
        }
    }

    // Helper methods from MerkleTreeTest2
    private static void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " Expected: " + expected + ", Actual: " + actual);
        }
    }

    private static void assertArrayEquals(byte[] expected, byte[] actual, String message) {
        if (!Arrays.equals(expected, actual)) {
            throw new AssertionError(message + " Expected: " + Arrays.toString(expected) + ", Actual: " + Arrays.toString(actual));
        }
    }

    private static void assertNotEquals(byte[] unexpected, byte[] actual, String message) {
        if (Arrays.equals(unexpected, actual)) {
            throw new AssertionError(message);
        }
    }

    // Delete old merkle trees
    public static void deleteOldMerkleTrees() {
        File folder = new File("merkleTree");
        deleteFolder(folder);
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) {
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    // Create a test hash with a given seed
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }
    
    private static void testLargeTreeUpdate() throws RocksDBException {
        System.out.println("Testing update of two large trees (30 leaves each)...");
        
        // Create source tree with 30 leaves
        MerkleTree sourceTree = new MerkleTree("largeSourceTree");
        try {
            // Add 30 leaves to source tree
            for (int i = 1; i <= 30; i++) {
                byte[] leafHash = createTestHash(i);
                sourceTree.addLeaf(sourceTree.new Node(leafHash));
            }
            sourceTree.flushToDisk();
            
            // Create target tree with 30 different leaves
            MerkleTree targetTree = new MerkleTree("largeTargetTree");
            try {
                // Add 30 leaves to target tree
                for (int i = 101; i <= 130; i++) {
                    byte[] leafHash = createTestHash(i);
                    targetTree.addLeaf(targetTree.new Node(leafHash));
                }
                targetTree.flushToDisk();
                
                // Save target root before merge
                byte[] targetRootBeforeMerge = targetTree.getRootHash();
                int targetLeavesBeforeMerge = targetTree.getNumLeaves();
                int targetDepthBeforeMerge = targetTree.getDepth();
                
                // Save source tree metadata
                byte[] sourceRoot = sourceTree.getRootHash();
                int sourceLeaves = sourceTree.getNumLeaves();
                int sourceDepth = sourceTree.getDepth();
                
                System.out.println("Source tree: " + sourceLeaves + " leaves, depth " + sourceDepth);
                System.out.println("Target tree before update: " + targetLeavesBeforeMerge + " leaves, depth " + targetDepthBeforeMerge);
                
                // Update target tree with source tree
                long startTime = System.currentTimeMillis();
                targetTree.updateWithTree(sourceTree);
                long endTime = System.currentTimeMillis();
                System.out.println("Update completed in " + (endTime - startTime) + "ms");
                
                // Verify target tree has been updated correctly
                System.out.println("Target tree after update: " + targetTree.getNumLeaves() + " leaves, depth " + targetTree.getDepth());
                
                // Root hash should match source tree
                assertArrayEquals(sourceRoot, targetTree.getRootHash(), 
                        "Target root should match source root after update");
                
                // Number of leaves should match source tree
                assertEquals(sourceLeaves, targetTree.getNumLeaves(), 
                        "Target numLeaves should match source numLeaves after update");
                
                // Depth should match source tree
                assertEquals(sourceDepth, targetTree.getDepth(), 
                        "Target depth should match source depth after update");
                
                // Root hash should be different from before update
                assertNotEquals(targetRootBeforeMerge, targetTree.getRootHash(), 
                        "Target root should change after update");
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    private static void testDifferentSizeTreeUpdate() throws RocksDBException {
        System.out.println("Testing update of trees with different leaf counts (30 and 45)...");
        
        // Create source tree with 45 leaves
        MerkleTree sourceTree = new MerkleTree("largeSourceTree2");
        try {
            // Add 45 leaves to source tree
            for (int i = 1; i <= 45; i++) {
                byte[] leafHash = createTestHash(i);
                sourceTree.addLeaf(sourceTree.new Node(leafHash));
            }
            sourceTree.flushToDisk();
            
            // Create target tree with 30 leaves
            MerkleTree targetTree = new MerkleTree("largeTargetTree2");
            try {
                // Add 30 leaves to target tree
                for (int i = 101; i <= 130; i++) {
                    byte[] leafHash = createTestHash(i);
                    targetTree.addLeaf(targetTree.new Node(leafHash));
                }
                targetTree.flushToDisk();
                
                // Save target root before merge
                byte[] targetRootBeforeMerge = targetTree.getRootHash();
                int targetLeavesBeforeMerge = targetTree.getNumLeaves();
                int targetDepthBeforeMerge = targetTree.getDepth();
                
                // Save source tree metadata
                byte[] sourceRoot = sourceTree.getRootHash();
                int sourceLeaves = sourceTree.getNumLeaves();
                int sourceDepth = sourceTree.getDepth();
                
                System.out.println("Source tree: " + sourceLeaves + " leaves, depth " + sourceDepth);
                System.out.println("Target tree before update: " + targetLeavesBeforeMerge + " leaves, depth " + targetDepthBeforeMerge);
                
                // Update target tree with source tree
                long startTime = System.currentTimeMillis();
                targetTree.updateWithTree(sourceTree);
                long endTime = System.currentTimeMillis();
                System.out.println("Update completed in " + (endTime - startTime) + "ms");
                
                // Verify target tree has been updated correctly
                System.out.println("Target tree after update: " + targetTree.getNumLeaves() + " leaves, depth " + targetTree.getDepth());
                
                // Root hash should match source tree
                assertArrayEquals(sourceRoot, targetTree.getRootHash(), 
                        "Target root should match source root after update");
                
                // Number of leaves should match source tree
                assertEquals(sourceLeaves, targetTree.getNumLeaves(), 
                        "Target numLeaves should match source numLeaves after update");
                
                // Depth should match source tree
                assertEquals(sourceDepth, targetTree.getDepth(), 
                        "Target depth should match source depth after update");
                
                // Root hash should be different from before update
                assertNotEquals(targetRootBeforeMerge, targetTree.getRootHash(), 
                        "Target root should change after update");
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
}
