package io.pwrlabs.database.rocksdb;

import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.ByteArrayWrapper;
import io.pwrlabs.util.encoders.Hex;
import io.pwrlabs.util.files.FileUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
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
    private static final String KEY_DATA_DB_NAME = "keyData";

    // Metadata Keys
    private static final String KEY_ROOT_HASH = "rootHash";
    private static final String KEY_NUM_LEAVES = "numLeaves";
    private static final String KEY_DEPTH = "depth";
    private static final String KEY_HANGING_NODE_PREFIX = "hangingNode";
    //endregion

    //region ===================== Fields =====================
    @Getter
    private final String treeName;
    private final String path;
    private RocksDB db;
    private ColumnFamilyHandle metaDataHandle;
    private ColumnFamilyHandle nodesHandle;
    private ColumnFamilyHandle keyDataHandle;


    /** Cache of loaded nodes (in-memory for quick access). */
    private final Map<ByteArrayWrapper, Node> nodesCache = new ConcurrentHashMap<>();

    private final Map<ByteArrayWrapper /*Key*/, byte[] /*data*/> keyDataCache = new ConcurrentHashMap<>();

    /** Lock for reading/writing to the tree. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<Integer /*level*/, byte[]> hangingNodes = new ConcurrentHashMap<>();
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
//            // Means this is a brand new DB -- no column families yet
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
        path = "merkleTree/" + treeName;
        File directory = new File(path);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new RocksDBException("Failed to create directory: " + path);
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
        cfDescriptors.add(new ColumnFamilyDescriptor(
                KEY_DATA_DB_NAME.getBytes(), cfOptions));

        // 5. Open DB with all column families
        this.db = RocksDB.open(dbOptions, path, cfDescriptors, cfHandles);

        // 6. Assign handles
        this.metaDataHandle = cfHandles.get(1);
        this.nodesHandle = cfHandles.get(2);
        this.keyDataHandle = cfHandles.get(3);

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
                hangingNodes.put(0, leafNode.hash);
                rootHash = leafNode.hash;
            } else {
                Node hangingLeaf = getNodeByHash(hangingNodes.get(0));

                // If there's no hanging leaf at level 0, place this one there.
                if (hangingLeaf == null) {
                    hangingNodes.put(0, leafNode.hash);

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
            Node hangingNode = getNodeByHash(hangingNodes.get(level));

            if (hangingNode == null) {
                // No hanging node at this level, so let's hang this node.
                hangingNodes.put(level, node.hash);

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
                if (parentNodeOfHangingNode != null) {
                    parentNodeOfHangingNode.addLeaf(node.hash);
                    hangingNodes.remove(level);
                } else {
                    // If parent node is null, create a new parent node
                    Node parent = new Node(hangingNode.hash, node.hash);
                    hangingNode.setParentNodeHash(parent.hash);
                    node.setParentNodeHash(parent.hash);
                    hangingNodes.remove(level);
                    addNode(level + 1, parent);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get all nodes saved on disk.
     * @return A list of all nodes in the tree
     * @throws RocksDBException If there's an error accessing RocksDB
     */
    public HashSet<Node> getAllNodes() throws RocksDBException {
        lock.writeLock().lock();
        try {
            HashSet<Node> allNodes = new HashSet<>();
            
            // First flush any pending changes to disk
            flushToDisk();
            
            // Use RocksIterator to iterate through all nodes in the nodesHandle column family
            try (RocksIterator iterator = db.newIterator(nodesHandle)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    byte[] nodeData = iterator.value();
                    
                    // Decode the node from its binary representation
                    Node node = decodeNode(nodeData);
                    allNodes.add(node);
                    
                    iterator.next();
                }
            }
            
            return allNodes;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get data for a key from the Merkle Tree.
     *
     * @param key The key to retrieve data for
     * @return The data for the key, or null if the key doesn't exist
     * @throws RocksDBException If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key is null
     */
    public byte[] getData(byte[] key) {
        byte[] data = keyDataCache.get(new ByteArrayWrapper(key));
        if(data != null) return data;

        try {
            return db.get(keyDataHandle, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Add or update data for a key in the Merkle Tree.
     * This will create a new leaf node with a hash derived from the key and data,
     * or update an existing leaf if the key already exists.
     *
     * @param key The key to store data for
     * @param data The data to store
     * @throws RocksDBException If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key or data is null
     */
    public void addOrUpdateData(byte[] key, byte[] data) throws RocksDBException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        
        lock.writeLock().lock();
        try {
            // Check if key already exists
            byte[] existingData = getData(key);
            
            // Calculate hash from key and data
            byte[] leafHash = PWRHash.hash256(key, data);
            
            // Store key-data mapping
            keyDataCache.put(new ByteArrayWrapper(key), data);
            
            if (existingData == null) {
                // Key doesn't exist, add new leaf
                addLeaf(new Node(leafHash));
            } else {
                // Key exists, update leaf
                // First get the old leaf hash
                byte[] oldLeafHash = PWRHash.hash256(key, existingData);
                updateLeaf(oldLeafHash, leafHash);
            }
            
            flushToDisk();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    public void revertUnsavedChanges() {
        lock.writeLock().lock();
        try {
            nodesCache.clear();
            hangingNodes.clear();
            keyDataCache.clear();

            loadMetaData();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Helper method to recursively delete a directory.
     */
    private void deleteDirectory(File directory) {
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

    public boolean containsKey(byte[] key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.readLock().lock();
        try {
            return db.get(keyDataHandle, key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<byte[]> getAllKeys() {
        lock.readLock().lock();
        try {
            List<byte[]> keys = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    keys.add(iterator.key());
                    iterator.next();
                }
            }
            return keys;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<byte[]> getAllData() {
        lock.readLock().lock();
        try {
            List<byte[]> data = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    data.add(iterator.value());
                    iterator.next();
                }
            }
            return data;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flush all in-memory changes (nodes, metadata) to RocksDB.
     */
    public void flushToDisk() throws RocksDBException {
        lock.writeLock().lock();
        try {
            try (WriteBatch batch = new WriteBatch()) {
                //Clear old metadata from disk
                try (RocksIterator iterator = db.newIterator(metaDataHandle)) {
                    iterator.seekToFirst();
                    while (iterator.isValid()) {
                        batch.delete(metaDataHandle, iterator.key());
                        iterator.next();
                    }
                }

                if (rootHash != null) {
                    batch.put(metaDataHandle, KEY_ROOT_HASH.getBytes(), rootHash);
                }
                batch.put(metaDataHandle, KEY_NUM_LEAVES.getBytes(), ByteBuffer.allocate(4).putInt(numLeaves).array());
                batch.put(metaDataHandle, KEY_DEPTH.getBytes(), ByteBuffer.allocate(4).putInt(depth).array());

                for (Map.Entry<Integer, byte[]> entry : hangingNodes.entrySet()) {
                    Integer level = entry.getKey();
                    byte[] nodeHash = entry.getValue();
                    batch.put(metaDataHandle, (KEY_HANGING_NODE_PREFIX + level).getBytes(), nodeHash);
                }

                for (Node node : nodesCache.values()) {
                    batch.put(nodesHandle, node.hash, node.encode());

                    if (node.getNodeHashToRemoveFromDb() != null) {
                        batch.delete(nodesHandle, node.getNodeHashToRemoveFromDb());
                    }
                }

                for(Map.Entry<ByteArrayWrapper, byte[]> entry : keyDataCache.entrySet()) {
                    batch.put(keyDataHandle, entry.getKey().data(), entry.getValue());
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
            
            if (keyDataHandle != null) {
                try {
                    keyDataHandle.close();
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

    /**
     * Efficiently clears the entire MerkleTree by closing, deleting and recreating the RocksDB instance.
     * This is much faster than iterating through all entries and deleting them individually.
     */
    /**
     * Efficiently clears the entire MerkleTree by closing, deleting and recreating the RocksDB instance.
     * This is much faster than iterating through all entries and deleting them individually.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            // First close the current DB
            if (!closed) {
                // Close all column family handles
                if (metaDataHandle != null) {
                    try {
                        metaDataHandle.close();
                    } catch (Exception e) {
                        // Log error but continue
                    }
                    metaDataHandle = null;
                }

                if (nodesHandle != null) {
                    try {
                        nodesHandle.close();
                    } catch (Exception e) {
                        // Log error but continue
                    }
                    nodesHandle = null;
                }

                if (keyDataHandle != null) {
                    try {
                        keyDataHandle.close();
                    } catch (Exception e) {
                        // Log error but continue
                    }
                    keyDataHandle = null;
                }

                if (db != null) {
                    try {
                        db.close();
                    } catch (Exception e) {
                        // Log error but continue
                    }
                }
            }

            // Clear in-memory structures
            nodesCache.clear();
            hangingNodes.clear();
            keyDataCache.clear();
            rootHash = null;
            numLeaves = 0;
            depth = 0;

            // Delete the directory
            File treeDir = new File("merkleTree/" + treeName);
            deleteDirectory(treeDir);

            // Re-open a fresh DB
            try {
                // Ensure directory exists
                if (!treeDir.exists() && !treeDir.mkdirs()) {
                    throw new RocksDBException("Failed to create directory: " + treeDir.getPath());
                }

                // Configure DB options
                DBOptions dbOptions = new DBOptions()
                        .setCreateIfMissing(true)
                        .setCreateMissingColumnFamilies(true)
                        .setParanoidChecks(true)
                        .setUseDirectReads(true)
                        .setUseDirectIoForFlushAndCompaction(true);

                // Re-apply all the options from the constructor
                dbOptions.setMaxTotalWalSize(45 * 1024 * 1024L)
                        .setWalSizeLimitMB(15)
                        .setWalTtlSeconds(24 * 60 * 60)
                        .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)
                        .setDbLogDir("")
                        .setLogFileTimeToRoll(0);

                dbOptions.setAllowMmapReads(false)
                        .setAllowMmapWrites(false)
                        .setMaxOpenFiles(1000)
                        .setMaxFileOpeningThreads(10);

                dbOptions.setEnableWriteThreadAdaptiveYield(true)
                        .setAllowConcurrentMemtableWrite(true);

                // Configure column family options
                ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                        .optimizeUniversalStyleCompaction()
                        .setWriteBufferSize(64 * 1024 * 1024L)
                        .setMaxWriteBufferNumber(3);

                // Prepare column families
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
                cfDescriptors.add(new ColumnFamilyDescriptor(
                        KEY_DATA_DB_NAME.getBytes(), cfOptions));

                // Open a fresh DB with all column families
                this.db = RocksDB.open(dbOptions, treeDir.getPath(), cfDescriptors, cfHandles);

                // Assign handles
                this.metaDataHandle = cfHandles.get(1);
                this.nodesHandle = cfHandles.get(2);
                this.keyDataHandle = cfHandles.get(3);

                // Reset closed flag
                closed = false;
            } catch (RocksDBException e) {
                throw new RuntimeException("Failed to re-initialize RocksDB after clearing: " + e.getMessage(), e);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public MerkleTree clone(String newTreeName) throws RocksDBException {
        if(newTreeName == null || newTreeName.isEmpty()) {
            throw new IllegalArgumentException("New tree name cannot be null or empty");
        }

        if (openTrees.containsKey(newTreeName)) {
            MerkleTree existingTree = openTrees.get(newTreeName);
            existingTree.close();
        }

        lock.writeLock().lock();
        try {
            flushToDisk();

            File origDir = new File(path);
            File destDir = new File("merkleTree/" + newTreeName);
            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                if (destDir.exists()) {
                    FileUtils.deleteDirectory(destDir);
                } else {
                    destDir.mkdirs();
                    FileUtils.deleteDirectory(destDir);
                }

                if (origDir.exists()) {
                    // Create a checkpoint and copy the state to the destination directory
                    checkpoint.createCheckpoint(destDir.getAbsolutePath());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            return new MerkleTree(newTreeName);
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
            hangingNodes.clear();
            for (int i = 0; i <= depth; i++) {
                byte[] hash = db.get(metaDataHandle, (KEY_HANGING_NODE_PREFIX + i).getBytes());
                if (hash != null) {
                    Node node = getNodeByHash(hash);
                    hangingNodes.put(i, node.hash);
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
    //endregion

    //region ===================== Nested Classes =====================
    /**
     * Represents a single node in the Merkle Tree.
     */
    @Getter
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

                byte[] oldHash = Arrays.copyOf(this.hash, this.hash.length);
                this.hash = newHash;

                for(Map.Entry<Integer, byte[]> entry : hangingNodes.entrySet()) {
                    if(Arrays.equals(entry.getValue(), oldHash)) {
                        hangingNodes.put(entry.getKey(), newHash);
                        break;
                    }
                }

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

        @Override
        public int hashCode() {
            return Arrays.hashCode(encode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Node)) {
                return false;
            }

            Node other = (Node) obj;

            // Compare hash - this is the most important field
            if(this.hash == null && other.hash != null) {
                return false;
            } else if(this.hash != null && other.hash == null) {
                return false;
            } else if(this.hash != null && other.hash != null) {
                if(!Arrays.equals(this.hash, other.hash)) {
                    return false;
                }
            }

            // Compare left child reference
            if(this.left == null && other.left != null) {
                return false;
            } else if(this.left != null && other.left == null) {
                return false;
            } else if(this.left != null && other.left != null) {
                if(!Arrays.equals(this.left, other.left)) {
                    return false;
                }
            }

            // Compare right child reference
            if(this.right == null && other.right != null) {
                return false;
            } else if(this.right != null && other.right == null) {
                return false;
            } else if(this.right != null && other.right != null) {
                if(!Arrays.equals(this.right, other.right)) {
                    return false;
                }
            }

            if(this.parent == null && other.parent != null) {
                return false;
            } else if(this.parent != null && other.parent == null) {
                return false;
            } else if(this.parent != null && other.parent != null) {
                if(!Arrays.equals(this.parent, other.parent)) {
                    return false;
                }
            }

            // Note: We don't compare parent references as they can legitimately differ
            // between source and cloned trees while still representing the same logical node

            return true;
        }
    }
    //endregion

    //region ===================== Equals =====================
    public boolean equals(MerkleTree other) {
        if (other == null) return false;

        try {
            // 1. Compare basic metadata
            if (!Arrays.equals(this.rootHash, other.rootHash)) return false;
            if (this.numLeaves != other.numLeaves) return false;
            if (this.depth != other.depth) return false;

            // 2. Compare node structure
            if (!compareAllNodes(other)) return false;

            // 3. Compare key-value data
            if (!compareKeyData(other)) return false;

            // 4. Compare hanging nodes
            if (!compareHangingNodes(other)) return false;

            return true;
        } catch (RocksDBException e) {
            // Log error if needed
            return false;
        }
    }

    private boolean compareAllNodes(MerkleTree other) throws RocksDBException {
        Set<Node> thisNodes = this.getAllNodes();
        Set<Node> otherNodes = other.getAllNodes();

        // Check node count match
        if (thisNodes.size() != otherNodes.size()) return false;

        // Check each node exists in both trees with same properties
        for (Node node : thisNodes) {
            Node otherNode = other.getNodeByHash(node.getHash());
            if (otherNode == null) return false;

            if (!Arrays.equals(node.getHash(), otherNode.getHash())) return false;
            if (!Arrays.equals(node.getLeft(), otherNode.getLeft())) return false;
            if (!Arrays.equals(node.getRight(), otherNode.getRight())) return false;
            if (!Arrays.equals(node.getParent(), otherNode.getParent())) return false;
        }

        return true;
    }

    private boolean compareKeyData(MerkleTree other) throws RocksDBException {
        Map<ByteArrayWrapper, byte[]> thisData = getAllKeyData();
        Map<ByteArrayWrapper, byte[]> otherData = other.getAllKeyData();

        if (thisData.size() != otherData.size()) return false;

        for (Map.Entry<ByteArrayWrapper, byte[]> entry : thisData.entrySet()) {
            byte[] otherValue = otherData.get(entry.getKey());
            if (!Arrays.equals(entry.getValue(), otherValue)) return false;
        }

        return true;
    }

    private boolean compareHangingNodes(MerkleTree other) {
        if (this.hangingNodes.size() != other.hangingNodes.size()) return false;

        for (Map.Entry<Integer, byte[]> entry : this.hangingNodes.entrySet()) {
            byte[] otherHash = other.hangingNodes.get(entry.getKey());
            if (!Arrays.equals(entry.getValue(), otherHash)) return false;
        }

        return true;
    }

    private Map<ByteArrayWrapper, byte[]> getAllKeyData() throws RocksDBException {
        Map<ByteArrayWrapper, byte[]> data = new HashMap<>();

        try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                data.put(
                        new ByteArrayWrapper(iterator.key()),
                        iterator.value()
                );
                iterator.next();
            }
        }

        return data;
    }
    //endregion
}
