package io.pwrlabs.database.rocksdb;

import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.ByteArrayWrapper;
import io.pwrlabs.util.encoders.Hex;
import lombok.Getter;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

/**
 * EdyMerkleTree: A Merkle Tree backed by RocksDB storage.
 */
public class MerkleTree {


    //region ===================== Statics =====================
    static {
        RocksDB.loadLibrary();
    }

    private static Map<String /*Tree Name*/, MerkleTree> openTrees = new ConcurrentHashMap<>();
    //endregion

    //region ===================== Constants =====================
    private static final int HASH_LENGTH = 32;
    private static final String METADATA_DB_NAME = "metaData";
    private static final String NODES_DB_NAME = "nodes";

    // Metadata Keys
    private static final String KEY_ROOT_HASH = "rootHash";
    private static final String KEY_NUM_LEAVES = "numLeaves";
    private static final String KEY_DEPTH = "depth";
    private static final String KEY_HANGING_NODE_PREFIX = "hangingNode";
    //endregion

    //region ===================== Fields =====================
    @Getter
    private final String treeName;
    private final RocksDB db;
    private ColumnFamilyHandle metaDataHandle;
    private ColumnFamilyHandle nodesHandle;


    /** Cache of loaded nodes (in-memory for quick access). */
    private final Map<ByteArrayWrapper, Node> nodesCache = new ConcurrentHashMap<>();

    /** Lock for reading/writing to the tree. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Integer /*level*/, Node> hangingNodes = new ConcurrentHashMap<>();
    @Getter
    private int numLeaves = 0;
    @Getter
    private int depth = 0;
    private byte[] rootHash = null;

    private boolean closed = false;
    //endregion

    //region ===================== Constructors =====================
//    @SneakyThrows
//    public MerkleTree(String treeName) {
//        errorIf(openTrees.containsKey(treeName), "There is already open instance of this tree. 2 open instances of the same tree are not allowed at the same time");
//        this.treeName = treeName;
//
//        Options options = new Options().setCreateIfMissing(true)
//                .setMaxTotalWalSize(45 * 1024 * 1024L)  // Good: Limits total WAL to 512MB
//                .setWalSizeLimitMB(15)                    // Good: Standard size per WAL file
//                .setWalTtlSeconds(24 * 60 * 60)           // Good: Cleans up old WAL files after 24h
//                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)  // Good: Minimizes logging overhead
//                .setDbLogDir("")                          // Good: Disables separate log directory
//                .setLogFileTimeToRoll(0);                 // Good: Immediate roll when size limit reached
//
//        // Add these additional safety options
//        options.setAllowMmapReads(false)  // Disable memory mapping
//                .setAllowMmapWrites(false)
//                .setMaxOpenFiles(1000)
//                .setMaxFileOpeningThreads(10)
//                .setIncreaseParallelism(1); // Single-threaded mode is safer
//
//        options.setParanoidChecks(true)  // Enable paranoid checks for corruption
//                .setUseDirectReads(true)  // Direct I/O for reads
//                .setUseDirectIoForFlushAndCompaction(true)  // Direct I/O for writes
//                .setEnableWriteThreadAdaptiveYield(true)
//                .setAllowConcurrentMemtableWrite(true);
//
//        File directory = new File("merkleTree/");
//        if(!directory.exists()) directory.mkdirs();
//
//        this.db = RocksDB.open(options, "merkleTree/" + treeName);
//
//        final ColumnFamilyDescriptor db1Descriptor =
//                new ColumnFamilyDescriptor(METADATA_DB_NAME.getBytes());
//        final ColumnFamilyDescriptor db2Descriptor =
//                new ColumnFamilyDescriptor(NODES_DB_NAME.getBytes());
//
//        metaDataHandle = db.createColumnFamily(db1Descriptor);
//        nodesHandle = db.createColumnFamily(db2Descriptor);
//
//        loadMetaData();
//
//        //Shutdown hook
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                    try {
//                        close();
//                    } catch (RocksDBException e) {
//                        e.printStackTrace();
//                    }
//                })
//        );
//
//        openTrees.put(treeName, this);
//    }

//    public MerkleTree(String treeName) throws RocksDBException {
//        RocksDB.loadLibrary();
//        this.treeName = treeName;
//        errorIf(openTrees.containsKey(treeName), "There is already open instance of this tree. 2 open instances of the same tree are not allowed at the same time");
//
//        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true)
//                .setMaxTotalWalSize(45 * 1024 * 1024L)  // Good: Limits total WAL to 512MB
//                .setWalSizeLimitMB(15)                    // Good: Standard size per WAL file
//                .setWalTtlSeconds(24 * 60 * 60)           // Good: Cleans up old WAL files after 24h
//                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)  // Good: Minimizes logging overhead
//                .setDbLogDir("")                          // Good: Disables separate log directory
//                .setLogFileTimeToRoll(0);                 // Good: Immediate roll when size limit reached
//
//        // Add these additional safety options
//        dbOptions.setAllowMmapReads(false)  // Disable memory mapping
//                .setAllowMmapWrites(false)
//                .setMaxOpenFiles(1000)
//                .setMaxFileOpeningThreads(10)
//                .setIncreaseParallelism(1); // Single-threaded mode is safer
//
//        dbOptions.setParanoidChecks(true)  // Enable paranoid checks for corruption
//                .setUseDirectReads(true)  // Direct I/O for reads
//                .setUseDirectIoForFlushAndCompaction(true)  // Direct I/O for writes
//                .setEnableWriteThreadAdaptiveYield(true)
//                .setAllowConcurrentMemtableWrite(true);
//        // set your other options here...
//
//        String dbPath = "merkleTree/" + treeName;
//
//
//        Options options = new Options().setCreateIfMissing(true)
//                .setMaxTotalWalSize(45 * 1024 * 1024L)  // Good: Limits total WAL to 512MB
//                .setWalSizeLimitMB(15)                    // Good: Standard size per WAL file
//                .setWalTtlSeconds(24 * 60 * 60)           // Good: Cleans up old WAL files after 24h
//                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)  // Good: Minimizes logging overhead
//                .setDbLogDir("")                          // Good: Disables separate log directory
//                .setLogFileTimeToRoll(0);                 // Good: Immediate roll when size limit reached
//
//        // Add these additional safety options
//        options.setAllowMmapReads(false)  // Disable memory mapping
//                .setAllowMmapWrites(false)
//                .setMaxOpenFiles(1000)
//                .setMaxFileOpeningThreads(10)
//                .setIncreaseParallelism(1); // Single-threaded mode is safer
//
//        options.setParanoidChecks(true)  // Enable paranoid checks for corruption
//                .setUseDirectReads(true)  // Direct I/O for reads
//                .setUseDirectIoForFlushAndCompaction(true)  // Direct I/O for writes
//                .setEnableWriteThreadAdaptiveYield(true)
//                .setAllowConcurrentMemtableWrite(true);
//
//        // 1) Figure out which column families already exist
//        List<byte[]> existingCFNames = RocksDB.listColumnFamilies(options, dbPath);
//
//        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
//        if (existingCFNames.isEmpty()) {
//            // Means this is a brand new DB – no column families yet
//            // We always need the default CF
//            cfDescriptors.add(new ColumnFamilyDescriptor(
//                    RocksDB.DEFAULT_COLUMN_FAMILY,
//                    new ColumnFamilyOptions())
//            );
//
//            // Also create metaData CF
//            cfDescriptors.add(new ColumnFamilyDescriptor(
//                    METADATA_DB_NAME.getBytes(),
//                    new ColumnFamilyOptions())
//            );
//
//            // Also create nodes CF
//            cfDescriptors.add(new ColumnFamilyDescriptor(
//                    NODES_DB_NAME.getBytes(),
//                    new ColumnFamilyOptions())
//            );
//        } else {
//            // We already have some (or all) CFs in the DB. We must open them *all*.
//            for (byte[] cfName : existingCFNames) {
//                cfDescriptors.add(
//                        new ColumnFamilyDescriptor(cfName, new ColumnFamilyOptions())
//                );
//            }
//        }
//
//        // 2) Open DB with all column family descriptors
//        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
//        this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
//
//        // 3) Figure out which handle corresponds to metaData, which to nodes
//        //    They come back in the same order we put them in cfDescriptors.
//        for (int i = 0; i < cfDescriptors.size(); i++) {
//            String cfName = new String(cfDescriptors.get(i).getName());
//            if (cfName.equals(METADATA_DB_NAME)) {
//                metaDataHandle = cfHandles.get(i);
//            } else if (cfName.equals(NODES_DB_NAME)) {
//                nodesHandle = cfHandles.get(i);
//            } else if (cfName.equals("default")) {
//                // If you need the default CF handle, grab it here
//            }
//        }
//
//        // If we found that we do NOT have metaDataHandle or nodesHandle yet (for example, an existing DB
//        // had only a default CF), you can create them here:
//        if (metaDataHandle == null) {
//            metaDataHandle = db.createColumnFamily(
//                    new ColumnFamilyDescriptor(METADATA_DB_NAME.getBytes(), new ColumnFamilyOptions())
//            );
//        }
//        if (nodesHandle == null) {
//            nodesHandle = db.createColumnFamily(
//                    new ColumnFamilyDescriptor(NODES_DB_NAME.getBytes(), new ColumnFamilyOptions())
//            );
//        }
//
//        // 4) Proceed as normal (e.g. load metadata, etc.)
//        loadMetaData();
//    }

    public MerkleTree(String treeName) throws RocksDBException {
        RocksDB.loadLibrary();
        this.treeName = treeName;
        errorIf(openTrees.containsKey(treeName), "There is already open instance of this tree");

        // 1. Ensure directory exists
        String dbPath = "merkleTree/" + treeName;
        File directory = new File(dbPath);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new RocksDBException("Failed to create directory: " + dbPath);
        }

        // 2. Configure DB options
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setParanoidChecks(true)
                .setUseDirectReads(true)
                .setUseDirectIoForFlushAndCompaction(true);

        dbOptions.setMaxTotalWalSize(45 * 1024 * 1024L)  // Good: Limits total WAL to 512MB
                .setWalSizeLimitMB(15)                    // Good: Standard size per WAL file
                .setWalTtlSeconds(24 * 60 * 60)           // Good: Cleans up old WAL files after 24h
                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)  // Good: Minimizes logging overhead
                .setDbLogDir("")                          // Good: Disables separate log directory
                .setLogFileTimeToRoll(0);                 // Good: Immediate roll when size limit reached

        // Add these additional safety options
        dbOptions.setAllowMmapReads(false)  // Disable memory mapping
                .setAllowMmapWrites(false)
                .setMaxOpenFiles(1000)
                .setMaxFileOpeningThreads(10); // Single-threaded mode is safer

        dbOptions.setUseDirectIoForFlushAndCompaction(true)  // Direct I/O for writes
                .setEnableWriteThreadAdaptiveYield(true)
                .setAllowConcurrentMemtableWrite(true);

        // 3. Configure column family options
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setWriteBufferSize(64 * 1024 * 1024L)  // 64MB memtable
                .setMaxWriteBufferNumber(3)   ;

        // 4. Prepare column families
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        // Always need default CF
        cfDescriptors.add(new ColumnFamilyDescriptor(
                RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));

        // Our custom CFs
        cfDescriptors.add(new ColumnFamilyDescriptor(
                METADATA_DB_NAME.getBytes(), cfOptions));
        cfDescriptors.add(new ColumnFamilyDescriptor(
                NODES_DB_NAME.getBytes(), cfOptions));

        // 5. Open DB with all column families
        this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);

        // 6. Assign handles
        this.metaDataHandle = cfHandles.get(1);
        this.nodesHandle = cfHandles.get(2);

        // 7. Load initial metadata
        loadMetaData();

        // 8. Register instance
        openTrees.put(treeName, this);

        //Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        close();
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                })
        );
    }

    //endregion

    //region ===================== Public Methods =====================
    /**
     * Add a new leaf node to the Merkle Tree.
     */
    public void addLeaf(Node leafNode) throws RocksDBException {
        if (leafNode == null) {
            throw new IllegalArgumentException("Leaf node cannot be null");
        }
        if (leafNode.hash == null) {
            throw new IllegalArgumentException("Leaf node hash cannot be null");
        }
        
        lock.writeLock().lock();
        try {
            if (numLeaves == 0) {
                hangingNodes.put(0, leafNode);
                rootHash = leafNode.hash;
            } else {
                Node hangingLeaf = hangingNodes.get(0);

                // If there's no hanging leaf at level 0, place this one there.
                if (hangingLeaf == null) {
                    hangingNodes.put(0, leafNode);

                    Node parentNode = new Node(leafNode.hash, null);
                    leafNode.setParentNodeHash(parentNode.hash);
                    addNode(1, parentNode);
                } else {
                    // If a leaf is already hanging, connect this leaf with the parent's parent
                    if (hangingLeaf.parent == null) { //If the hanging leaf is the root
                        Node parentNode = new Node(hangingLeaf.hash, leafNode.hash);
                        hangingLeaf.setParentNodeHash(parentNode.hash);
                        leafNode.setParentNodeHash(parentNode.hash);
                        addNode(1, parentNode);
                    } else {
                        Node parentNodeOfHangingLeaf = getNodeByHash(hangingLeaf.parent);
                        if (parentNodeOfHangingLeaf == null) {
                            throw new IllegalStateException("Parent node of hanging leaf not found");
                        }
                        parentNodeOfHangingLeaf.addLeaf(leafNode.hash);
                    }
                    hangingNodes.remove(0);
                }
            }

            numLeaves++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void addLeafIfMissing(byte[] leafHash) {
        if (leafHash == null) {
            throw new IllegalArgumentException("Leaf hash cannot be null");
        }
        
        lock.writeLock().lock();
        try {
            if (getNodeByHash(leafHash) == null) {
                addLeaf(new Node(leafHash));
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
        if (oldLeafHash == null) {
            throw new IllegalArgumentException("Old leaf hash cannot be null");
        }
        if (newLeafHash == null) {
            throw new IllegalArgumentException("New leaf hash cannot be null");
        }
        
        lock.writeLock().lock();
        try {
            Node leaf = getNodeByHash(oldLeafHash);

            if(leaf == null) {
                throw new IllegalArgumentException("Leaf not found: " + Hex.toHexString(oldLeafHash));
            } else {
                leaf.updateNodeHash(newLeafHash);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Error updating leaf: " + e.getMessage(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Add a node at a given level.
     */
    public void addNode(int level, Node node) throws RocksDBException {
        lock.writeLock().lock();
        try {
            if (level > depth) depth = level;
            Node hangingNode = hangingNodes.get(level);

            if (hangingNode == null) {
                // No hanging node at this level, so let's hang this node.
                hangingNodes.put(level, node);

                // If this level is the depth level, this node's hash is the new root hash
                if (level >= depth) {
                    rootHash = node.hash;
                } else {
                    // Otherwise, create a parent and keep going up
                    Node parentNode = new Node(node.hash, null);
                    node.setParentNodeHash(parentNode.hash);
                    addNode(level + 1, parentNode);
                }
            } else if(hangingNode.parent == null) {
                Node parent = new Node(hangingNode.hash, node.hash);
                hangingNode.setParentNodeHash(parent.hash);
                node.setParentNodeHash(parent.hash);
                hangingNodes.remove(level);
                addNode(level + 1, parent);
            } else {
                // If a node is already hanging at this level, attach the new node as a leaf
                Node parentNodeOfHangingNode = getNodeByHash(hangingNode.parent);
                parentNodeOfHangingNode.addLeaf(node.hash);
                hangingNodes.remove(level);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Flush all in-memory changes (nodes, metadata) to RocksDB.
     */
    public void flushToDisk() throws RocksDBException {
        lock.writeLock().lock();
        try {
            try (WriteBatch batch = new WriteBatch()) {
                if (rootHash != null) {
                    batch.put(metaDataHandle, KEY_ROOT_HASH.getBytes(), rootHash);
                }
                batch.put(metaDataHandle, KEY_NUM_LEAVES.getBytes(), ByteBuffer.allocate(4).putInt(numLeaves).array());
                batch.put(metaDataHandle, KEY_DEPTH.getBytes(), ByteBuffer.allocate(4).putInt(depth).array());

                for (Map.Entry<Integer, Node> entry : hangingNodes.entrySet()) {
                    Integer level = entry.getKey();
                    Node node = entry.getValue();
                    batch.put(metaDataHandle, (KEY_HANGING_NODE_PREFIX + level).getBytes(), node.hash);
                }

                for (Node node : nodesCache.values()) {
                    batch.put(nodesHandle, node.hash, node.encode());

                    if (node.getNodeHashToRemoveFromDb() != null) {
                        batch.delete(nodesHandle, node.getNodeHashToRemoveFromDb());
                    }
                }

                try (WriteOptions writeOptions = new WriteOptions()) {
                    db.write(writeOptions, batch);
                }

                nodesCache.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the current root hash of the Merkle tree.
     */
    public byte[] getRootHash() {
        return rootHash;
    }

    public void revertUnsavedChanges() {
        lock.writeLock().lock();
        try {
            nodesCache.clear();
            hangingNodes.clear();

            loadMetaData();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Efficiently update this tree with nodes from another tree by recursively
     * comparing subtrees and only updating where differences exist.
     *
     * @param sourceTree The source tree to update from
     * @return Number of nodes updated
     * @throws RocksDBException If there's an error accessing RocksDB
     */
    public void updateWithTree(MerkleTree sourceTree) throws RocksDBException {
        if (sourceTree == null || sourceTree.getRootHash() == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            flushToDisk();

            // If current tree is empty, just use the source tree's structure
            if (this.rootHash == null || depth < 3) {
                copyEntireTree(sourceTree);
                return; // At least one node updated (the root)
            }

            // Compare trees and update nodes where needed
            compareAndUpdateNodes(this.rootHash, sourceTree.rootHash, sourceTree);

            //Copy metadata
            this.numLeaves = sourceTree.numLeaves;
            this.depth = sourceTree.depth;
            this.rootHash = sourceTree.getRootHash(); // Ensure root hash matches source tree

            //Copy hanging nodes
            hangingNodes.clear();
            for (Map.Entry<Integer, Node> entry : sourceTree.hangingNodes.entrySet()) {
                Integer level = entry.getKey();
                byte[] nodeHash = entry.getValue().hash;

                Node node = getNodeByHash(nodeHash);
                if (node != null) {
                    hangingNodes.put(level, node);
                } else {
                    // If node doesn't exist in target tree, create it
                    Node sourceNode = entry.getValue();
                    try {
                        node = copySubtree(sourceNode, sourceTree);
                        if (node != null) {
                            hangingNodes.put(level, node);
                        }
                    } catch (RocksDBException e) {
                        throw new RuntimeException("Error copying hanging node: " + e.getMessage(), e);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Compare this tree with another tree to verify they have identical structure.
     * This performs a full comparison of all nodes, their relationships, and hashes.
     *
     * @param otherTree The tree to compare with
     * @return true if the trees match exactly, false otherwise
     * @throws RocksDBException If there's an error accessing RocksDB
     */
    public boolean compareWithTree(MerkleTree otherTree) throws RocksDBException {
        if (otherTree == null) {
            return false;
        }
        
        lock.readLock().lock();
        try {
            // Compare metadata
            if (this.numLeaves != otherTree.numLeaves || this.depth != otherTree.depth) {
                return false;
            }
            
            // Compare root hashes
            if (!Arrays.equals(this.rootHash, otherTree.rootHash)) {
                return false;
            }
            
            // If both trees are empty, they match
            if (this.rootHash == null && otherTree.rootHash == null) {
                return true;
            }
            
            // Compare hanging nodes
            if (this.hangingNodes.size() != otherTree.hangingNodes.size()) {
                return false;
            }
            
            for (Map.Entry<Integer, Node> entry : this.hangingNodes.entrySet()) {
                Integer level = entry.getKey();
                Node thisNode = entry.getValue();
                Node otherNode = otherTree.hangingNodes.get(level);
                
                if (otherNode == null || !Arrays.equals(thisNode.hash, otherNode.hash)) {
                    return false;
                }
            }
            
            // Recursively compare all nodes starting from the root
            return compareNodes(this.rootHash, otherTree.rootHash, otherTree);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Recursively compare nodes from both trees.
     */
    private boolean compareNodes(byte[] thisNodeHash, byte[] otherNodeHash, 
                                MerkleTree otherTree) throws RocksDBException {
        // If both hashes are null, nodes match
        if (thisNodeHash == null && otherNodeHash == null) {
            return true;
        }
        
        // If only one hash is null, nodes don't match
        if (thisNodeHash == null || otherNodeHash == null) {
            return false;
        }
        
        // If hashes are equal, check node structure
        if (!Arrays.equals(thisNodeHash, otherNodeHash)) {
            return false;
        }
        
        // Get nodes from both trees
        Node thisNode = getNodeByHash(thisNodeHash);
        Node otherNode = otherTree.getNodeByHash(otherNodeHash);
        
        if (thisNode == null || otherNode == null) {
            return false;
        }
        
        // Compare left children
        boolean leftMatch = compareNodes(thisNode.left, otherNode.left, otherTree);
        if (!leftMatch) {
            return false;
        }
        
        // Compare right children
        boolean rightMatch = compareNodes(thisNode.right, otherNode.right, otherTree);
        if (!rightMatch) {
            return false;
        }
        
        // If we got here, nodes match
        return true;
    }

    /**
     * Close the databases (optional, if you need cleanup).
     */
    public void close() throws RocksDBException {
        lock.writeLock().lock();
        try {
            if(closed) return;
            flushToDisk();

            if (metaDataHandle != null) {
                try {
                    metaDataHandle.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            if (nodesHandle != null) {
                try {
                    nodesHandle.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            if (db != null) {
                try {
                    db.close();
                } catch (Exception e) {
                    // Log error
                }
            }

            openTrees.remove(treeName);
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }
    //endregion

    //region ===================== Private Methods =====================
    /**
     * Load the tree's metadata from RocksDB.
     */
    private void loadMetaData() throws RocksDBException {
        lock.readLock().lock();
        try {
            this.rootHash = db.get(metaDataHandle, KEY_ROOT_HASH.getBytes());

            byte[] numLeavesBytes = db.get(metaDataHandle, KEY_NUM_LEAVES.getBytes());
            this.numLeaves = (numLeavesBytes == null)
                    ? 0
                    : ByteBuffer.wrap(numLeavesBytes).getInt();

            byte[] depthBytes = db.get(metaDataHandle, KEY_DEPTH.getBytes());
            this.depth = (depthBytes == null)
                    ? 0
                    : ByteBuffer.wrap(depthBytes).getInt();

            // Load any hangingNodes from metadata
            for (int i = 0; i <= depth; i++) {
                byte[] hash = db.get(metaDataHandle, (KEY_HANGING_NODE_PREFIX + i).getBytes());
                if (hash != null) {
                    Node node = getNodeByHash(hash);
                    hangingNodes.put(i, node);
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Fetch a node by its hash, either from the in-memory cache or from RocksDB.
     */
    private Node getNodeByHash(byte[] hash) throws RocksDBException {
        lock.readLock().lock();
        try {
            if (hash == null) return null;

            ByteArrayWrapper baw = new ByteArrayWrapper(hash);
            Node node = nodesCache.get(baw);
            if (node == null) {
                try {
                    byte[] encodedData = db.get(nodesHandle, hash);
                    if (encodedData == null) {
                        return null;
                    }
                    node = decodeNode(encodedData);
                    nodesCache.put(baw, node);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }

            return node;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Decode a node from bytes.
     */
    private Node decodeNode(byte[] encodedData) {
        ByteBuffer buffer = ByteBuffer.wrap(encodedData);

        byte[] hash = new byte[HASH_LENGTH];
        buffer.get(hash);

        boolean hasLeft = buffer.get() == 1;
        boolean hasRight = buffer.get() == 1;
        boolean hasParent = buffer.get() == 1;

        byte[] left = hasLeft ? new byte[HASH_LENGTH] : null;
        byte[] right = hasRight ? new byte[HASH_LENGTH] : null;
        byte[] parent = hasParent ? new byte[HASH_LENGTH] : null;

        if (hasLeft)  buffer.get(left);
        if (hasRight) buffer.get(right);
        if (hasParent)buffer.get(parent);

        return new Node(hash, left, right, parent);
    }

    /**
     * Recursively compare nodes from both trees and update as needed.
     */
    private void compareAndUpdateNodes(byte[] currentNodeHash, byte[] sourceNodeHash,
                                       MerkleTree sourceTree) throws RocksDBException {
        // If hashes are equal, subtrees are identical - no update needed
        if (Arrays.equals(currentNodeHash, sourceNodeHash)) {
            return;
        }

        // Get nodes from both trees
        Node currentNode = getNodeByHash(currentNodeHash);
        Node sourceNode = sourceTree.getNodeByHash(sourceNodeHash);

        if (currentNode == null || sourceNode == null) {
            return;
        }

        // If it's a leaf node, update it
        if (sourceNode.left == null && sourceNode.right == null) {
            // Found a different leaf - update it
            if (currentNode.left == null && currentNode.right == null) {
                currentNode.updateNodeHash(sourceNode.hash);
            }
            return;
        }

        // Compare and update left children if they exist
        if (sourceNode.left != null) {
            if (currentNode.left != null) {
                compareAndUpdateNodes(currentNode.left, sourceNode.left, sourceTree);
            } else {
                // Current node doesn't have a left child, but source does
                Node leftNode = sourceTree.getNodeByHash(sourceNode.left);
                if (leftNode != null) {
                    // Need to copy this subtree
                    Node newNode = copySubtree(leftNode, sourceTree);
                    currentNode.addLeaf(newNode.hash);
                }
            }
        }

        // Compare and update right children if they exist
        if (sourceNode.right != null) {
            if (currentNode.right != null) {
                compareAndUpdateNodes(currentNode.right, sourceNode.right, sourceTree);
            } else {
                // Current node doesn't have a right child, but source does
                Node rightNode = sourceTree.getNodeByHash(sourceNode.right);
                if (rightNode != null) {
                    // Need to copy this subtree
                    Node newNode = copySubtree(rightNode, sourceTree);
                    currentNode.addLeaf(newNode.hash);
                }
            }
        }
    }

    /**
     * Copy an entire subtree from the source tree.
     */
    private Node copySubtree(Node sourceNode, MerkleTree sourceTree) throws RocksDBException {
        if (sourceNode == null) {
            return null;
        }

        // Check if this node already exists in our tree
        Node existingNode = getNodeByHash(sourceNode.hash);
        if (existingNode != null) {
            return existingNode;
        }

        // Create a new node
        Node newNode;

        // If it's a leaf, just copy the hash
        if (sourceNode.left == null && sourceNode.right == null) {
            newNode = new Node(sourceNode.hash);
        } else {
            // Otherwise recursively copy children
            byte[] leftHash = null;
            byte[] rightHash = null;

            if (sourceNode.left != null) {
                Node sourceLeft = sourceTree.getNodeByHash(sourceNode.left);
                Node newLeft = copySubtree(sourceLeft, sourceTree);
                if (newLeft != null) {
                    leftHash = newLeft.hash;
                }
            }

            if (sourceNode.right != null) {
                Node sourceRight = sourceTree.getNodeByHash(sourceNode.right);
                Node newRight = copySubtree(sourceRight, sourceTree);
                if (newRight != null) {
                    rightHash = newRight.hash;
                }
            }

            // Create new internal node
            newNode = new Node(leftHash, rightHash);
        }

        return newNode;
    }

    /**
     * Copy the entire structure of the source tree when this tree is empty.
     */
    private void copyEntireTree(MerkleTree sourceTree) throws RocksDBException {
        if (sourceTree.getRootHash() == null) {
            return;
        }

        // Recursively copy starting from the root
        Node sourceRoot = sourceTree.getNodeByHash(sourceTree.getRootHash());
        if (sourceRoot != null) {
            copySubtree(sourceRoot, sourceTree);
            this.rootHash = sourceTree.getRootHash();
            this.depth = sourceTree.getDepth();
            this.numLeaves = sourceTree.getNumLeaves();

            // Update hanging nodes
            for (Map.Entry<Integer, Node> entry : sourceTree.hangingNodes.entrySet()) {
                this.hangingNodes.put(entry.getKey(), getNodeByHash(entry.getValue().hash));
            }

            flushToDisk();
        }
    }

    //endregion

    //region ===================== Nested Classes =====================
    /**
     * Represents a single node in the Merkle Tree.
     */
    public class Node {
        private byte[] hash;
        private byte[] left;
        private byte[] right;
        private byte[] parent;

        /**
         *  The old hash of the node before it was updated. This is used to delete the old node from the db.
         * */
        @Getter
       private byte[] nodeHashToRemoveFromDb = null;

        /**
         * Construct a leaf node with a known hash.
         */
        public Node(byte[] hash) {
            if (hash == null) {
                throw new IllegalArgumentException("Node hash cannot be null");
            }
            this.hash = hash;

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Construct a node with all fields.
         */
        public Node(byte[] hash, byte[] left, byte[] right, byte[] parent) {
            if (hash == null) {
                throw new IllegalArgumentException("Node hash cannot be null");
            }
            this.hash = hash;
            this.left = left;
            this.right = right;
            this.parent = parent;

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Construct a node (non-leaf) with leftHash and rightHash, auto-calculate node hash.
         */
        public Node(byte[] left, byte[] right) {
            // At least one of left or right must be non-null
            if (left == null && right == null) {
                throw new IllegalArgumentException("At least one of left or right hash must be non-null");
            }
            
            this.left = left;
            this.right = right;
            this.hash = calculateHash();
            
            if (this.hash == null) {
                throw new IllegalStateException("Failed to calculate node hash");
            }

            nodesCache.put(new ByteArrayWrapper(hash), this);
        }

        /**
         * Calculate the hash of this node based on the left and right child hashes.
         */
        public byte[] calculateHash() {
            lock.readLock().lock();
            try {
                if (left == null && right == null) {
                    // Could be a leaf that hasn't set its hash, or zero-length node
                    return null;
                }

                byte[] leftHash = (left != null) ? left : right; // Duplicate if necessary
                byte[] rightHash = (right != null) ? right : left;
                return PWRHash.hash256(leftHash, rightHash);
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Encode the node into a byte[] for storage in RocksDB.
         */
        public byte[] encode() {
            lock.readLock().lock();
            try {
                boolean hasLeft = (left != null);
                boolean hasRight = (right != null);
                boolean hasParent = (parent != null);

                int length = HASH_LENGTH + 3 // flags for hasLeft, hasRight, hasParent
                        + (hasLeft ? HASH_LENGTH : 0)
                        + (hasRight ? HASH_LENGTH : 0)
                        + (hasParent ? HASH_LENGTH : 0);

                ByteBuffer buffer = ByteBuffer.allocate(length);
                buffer.put(hash);
                buffer.put(hasLeft ? (byte) 1 : (byte) 0);
                buffer.put(hasRight ? (byte) 1 : (byte) 0);
                buffer.put(hasParent ? (byte) 1 : (byte) 0);

                if (hasLeft) buffer.put(left);
                if (hasRight) buffer.put(right);
                if (hasParent) buffer.put(parent);

                return buffer.array();
            } finally {
                lock.readLock().unlock();
            }
        }

        /**
         * Set this node's parent.
         */
        public void setParentNodeHash(byte[] parentHash) {
            lock.writeLock().lock();
            try {
                this.parent = parentHash;
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Update this node's hash and propagate the change upward.
         */
        public void updateNodeHash(byte[] newHash) throws RocksDBException {
            lock.writeLock().lock();
            try {
                //We only store the old hash if it is not already set. Because we only need to delete the old node from the db once.
                //New hashes don't have copies in db since they haven't been flushed to disk yet
                if (nodeHashToRemoveFromDb == null) nodeHashToRemoveFromDb = this.hash;

                byte[] oldHash = this.hash;
                this.hash = newHash;

                nodesCache.remove(new ByteArrayWrapper(oldHash));
                nodesCache.put(new ByteArrayWrapper(newHash), this);

                // Distinguish whether it is a leaf, internal node, or root
                boolean isLeaf = (left == null && right == null);
                boolean isRoot = (parent == null);

                // If this is the root node, update the root hash
                if (isRoot) {
                    rootHash = newHash;
                    
                    if(left != null) {
                        Node leftNode = getNodeByHash(left);
                        if (leftNode != null) {
                            leftNode.setParentNodeHash(newHash);
                        }
                    }

                    if(right != null) {
                        Node rightNode = getNodeByHash(right);
                        if (rightNode != null) {
                            rightNode.setParentNodeHash(newHash);
                        }
                    }
                }

                // If this is a leaf node with a parent, update the parent
                if (isLeaf && !isRoot) {
                    Node parentNode = getNodeByHash(parent);
                    if (parentNode != null) {
                        parentNode.updateLeaf(oldHash, newHash);
                        byte[] newParentHash = parentNode.calculateHash();
                        parentNode.updateNodeHash(newParentHash);
                    }
                } 
                // If this is an internal node with a parent, update the parent and children
                else if (!isLeaf && !isRoot) {
                    if (left != null) {
                        Node leftNode = getNodeByHash(left);
                        if (leftNode != null) {
                            leftNode.setParentNodeHash(newHash);
                        }
                    }
                    if (right != null) {
                        Node rightNode = getNodeByHash(right);
                        if (rightNode != null) {
                            rightNode.setParentNodeHash(newHash);
                        }
                    }

                    Node parentNode = getNodeByHash(parent);
                    if (parentNode != null) {
                        parentNode.updateLeaf(oldHash, newHash);
                        byte[] newParentHash = parentNode.calculateHash();
                        parentNode.updateNodeHash(newParentHash);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Update a leaf (left or right) if it matches the old hash.
         */
        public void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
            lock.writeLock().lock();
            try {
                if (left != null && Arrays.equals(left, oldLeafHash)) {
                    left = newLeafHash;
                } else if (right != null && Arrays.equals(right, oldLeafHash)) {
                    right = newLeafHash;
                } else {
                    throw new IllegalArgumentException("Old hash not found among this node's children");
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * Add a leaf to this node (either left or right).
         */
        public void addLeaf(byte[] leafHash) throws RocksDBException {
            if (leafHash == null) {
                throw new IllegalArgumentException("Leaf hash cannot be null");
            }
            
            lock.writeLock().lock();
            try {
                Node leafNode = getNodeByHash(leafHash);
                if(leafNode == null) {
                    throw new IllegalArgumentException("Leaf node not found: " + Hex.toHexString(leafHash));
                }

                if (left == null) {
                    left = leafHash;
                } else if (right == null) {
                    right = leafHash;
                } else {
                    throw new IllegalArgumentException("Node already has both left and right children");
                }

                byte[] newHash = calculateHash();
                if (newHash == null) {
                    throw new IllegalStateException("Failed to calculate new hash after adding leaf");
                }
                updateNodeHash(newHash);
            } finally {
                lock.writeLock().unlock();
            }
        }
    }
    //endregion
}
