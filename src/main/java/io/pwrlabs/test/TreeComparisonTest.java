package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class TreeComparisonTest {

    public static void main(String[] args) {
        try {
            deleteOldMerkleTrees();
            
            testIdenticalTrees();
            System.out.println("testIdenticalTrees passed");
            
            testDifferentTrees();
            System.out.println("testDifferentTrees passed");
            
            testTreeUpdateComparison();
            System.out.println("testTreeUpdateComparison passed");
            
            testDifferentSizeTrees();
            System.out.println("testDifferentSizeTrees passed");
            
            System.out.println("All tests passed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Tests failed: " + e.getMessage());
        }
    }
    
    // Delete old merkle trees
    public static void deleteOldMerkleTrees() {
        File folder = new File("merkleTree");
        deleteFolder(folder);
    }
    
    public static void deleteFolder(File folder) {
        if (folder.exists()) {
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
    }
    
    // Create a test hash with a given seed
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }
    
    private static void testIdenticalTrees() throws RocksDBException {
        System.out.println("Testing comparison of identical trees...");
        
        // Create first tree with 10 leaves
        MerkleTree tree1 = new MerkleTree("identicalTree1");
        try {
            // Add 10 leaves to tree1
            for (int i = 1; i <= 10; i++) {
                byte[] leafHash = createTestHash(i);
                tree1.addLeaf(tree1.new Node(leafHash));
            }
            tree1.flushToDisk();
            
            // Create second tree with the same 10 leaves
            MerkleTree tree2 = new MerkleTree("identicalTree2");
            try {
                // Add the same 10 leaves to tree2
                for (int i = 1; i <= 10; i++) {
                    byte[] leafHash = createTestHash(i);
                    tree2.addLeaf(tree2.new Node(leafHash));
                }
                tree2.flushToDisk();
                
                // Compare trees
                boolean treesMatch = tree1.compareWithTree(tree2);
                
                if (!treesMatch) {
                    throw new AssertionError("Identical trees should match");
                }
            } finally {
                tree2.close();
            }
        } finally {
            tree1.close();
        }
    }
    
    private static void testDifferentTrees() throws RocksDBException {
        System.out.println("Testing comparison of different trees...");
        
        // Create first tree with 10 leaves
        MerkleTree tree1 = new MerkleTree("differentTree1");
        try {
            // Add 10 leaves to tree1
            for (int i = 1; i <= 10; i++) {
                byte[] leafHash = createTestHash(i);
                tree1.addLeaf(tree1.new Node(leafHash));
            }
            tree1.flushToDisk();
            
            // Create second tree with different 10 leaves
            MerkleTree tree2 = new MerkleTree("differentTree2");
            try {
                // Add different 10 leaves to tree2
                for (int i = 11; i <= 20; i++) {
                    byte[] leafHash = createTestHash(i);
                    tree2.addLeaf(tree2.new Node(leafHash));
                }
                tree2.flushToDisk();
                
                // Compare trees
                boolean treesMatch = tree1.compareWithTree(tree2);
                
                if (treesMatch) {
                    throw new AssertionError("Different trees should not match");
                }
            } finally {
                tree2.close();
            }
        } finally {
            tree1.close();
        }
    }
    
    private static void testTreeUpdateComparison() throws RocksDBException {
        System.out.println("Testing comparison after tree update...");
        
        // Create source tree with 10 leaves
        MerkleTree sourceTree = new MerkleTree("updateSource");
        try {
            // Add 10 leaves to source tree
            for (int i = 1; i <= 10; i++) {
                byte[] leafHash = createTestHash(i);
                sourceTree.addLeaf(sourceTree.new Node(leafHash));
            }
            sourceTree.flushToDisk();
            
            // Create target tree with different 10 leaves
            MerkleTree targetTree = new MerkleTree("updateTarget");
            try {
                // Add different 10 leaves to target tree
                for (int i = 11; i <= 20; i++) {
                    byte[] leafHash = createTestHash(i);
                    targetTree.addLeaf(targetTree.new Node(leafHash));
                }
                targetTree.flushToDisk();
                
                // Verify trees are different before update
                boolean treesMatchBeforeUpdate = sourceTree.compareWithTree(targetTree);
                if (treesMatchBeforeUpdate) {
                    throw new AssertionError("Trees should not match before update");
                }
                
                // Update target tree with source tree
                targetTree.updateWithTree(sourceTree);
                
                // Verify trees match after update
                boolean treesMatchAfterUpdate = sourceTree.compareWithTree(targetTree);
                if (!treesMatchAfterUpdate) {
                    throw new AssertionError("Trees should match after update");
                }
            } finally {
                targetTree.close();
            }
        } finally {
            sourceTree.close();
        }
    }
    
    private static void testDifferentSizeTrees() throws RocksDBException {
        System.out.println("Testing comparison of trees with different sizes...");
        
        // Create source tree with 30 leaves
        MerkleTree smallTree = new MerkleTree("smallTree");
        try {
            // Add 30 leaves to small tree
            for (int i = 1; i <= 30; i++) {
                byte[] leafHash = createTestHash(i);
                smallTree.addLeaf(smallTree.new Node(leafHash));
            }
            smallTree.flushToDisk();
            
            // Create target tree with 45 leaves
            MerkleTree largeTree = new MerkleTree("largeTree");
            try {
                // Add 45 leaves to large tree
                for (int i = 1; i <= 45; i++) {
                    byte[] leafHash = createTestHash(i);
                    largeTree.addLeaf(largeTree.new Node(leafHash));
                }
                largeTree.flushToDisk();
                
                // Verify trees are different
                boolean treesMatch = smallTree.compareWithTree(largeTree);
                if (treesMatch) {
                    throw new AssertionError("Trees with different sizes should not match");
                }
                
                // Update small tree with large tree
                smallTree.updateWithTree(largeTree);
                
                // Verify trees match after update
                boolean treesMatchAfterUpdate = smallTree.compareWithTree(largeTree);
                if (!treesMatchAfterUpdate) {
                    throw new AssertionError("Trees should match after update");
                }
            } finally {
                largeTree.close();
            }
        } finally {
            smallTree.close();
        }
    }
}
