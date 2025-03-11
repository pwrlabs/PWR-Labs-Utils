package io.pwrlabs.test;

import io.pwrlabs.database.rocksdb.MerkleTree;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Arrays;

public class HangingNodePersistenceTest {

    public static void main(String[] args) {
        try {
            System.out.println("Testing hanging node persistence...");
            
            // Create a new tree
            String treeName = "hangingNodeTest";
            
            // First, ensure any existing test tree is deleted
            deleteTree(treeName);
            
            // Create a new tree and add some data
            System.out.println("Creating new tree and adding data...");
            createTreeWithData(treeName);
            
            // Now try to reopen the tree without deleting it
            System.out.println("Reopening tree without deleting it...");
            MerkleTree reopenedTree = new MerkleTree(treeName);
            
            System.out.println("Tree reopened successfully!");
            
            // Clean up
            reopenedTree.close();
            deleteTree(treeName);
            
            System.out.println("Test passed!");
        } catch (Exception e) {
            System.out.println("Test failed with exception: ");
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void createTreeWithData(String treeName) throws RocksDBException {
        MerkleTree tree = new MerkleTree(treeName);
        
        // Add some leaves to create hanging nodes
        for (int i = 1; i <= 10; i++) {
            byte[] leafHash = createTestHash(i);
            tree.addLeaf(tree.new Node(leafHash));
        }
        
        // Add some key-value data
        for (int i = 1; i <= 5; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] data = ("data" + i).getBytes();
            tree.addOrUpdateData(key, data);
        }
        
        // Flush changes to disk
        tree.flushToDisk();
        
        // Close the tree
        tree.close();
    }
    
    private static byte[] createTestHash(int seed) {
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) seed);
        return hash;
    }
    
    private static void deleteTree(String treeName) {
        File treeDir = new File("merkleTree/" + treeName);
        if (treeDir.exists()) {
            deleteDirectory(treeDir);
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
