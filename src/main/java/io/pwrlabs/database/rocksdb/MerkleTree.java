package io.pwrlabs.database.rocksdb;

import io.pwrlabs.hashing.PWRHash;
import io.pwrlabs.util.encoders.BiResult;
import io.pwrlabs.util.encoders.ByteArrayWrapper;
import io.pwrlabs.util.encoders.Hex;
import io.pwrlabs.util.files.FileUtils;
import io.pwrlabs.utils.ObjectsInMemoryTracker;
import lombok.Getter;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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
    @Getter
    private final String path;

    private RocksDB db;
    private ColumnFamilyHandle metaDataHandle;
    private ColumnFamilyHandle nodesHandle;
    private ColumnFamilyHandle keyDataHandle;


    /**
     * Cache of loaded nodes (in-memory for quick access).
     */
    private final Map<ByteArrayWrapper, Node> nodesCache = new ConcurrentHashMap<>();
    private final Map<Integer /*level*/, byte[]> hangingNodes = new ConcurrentHashMap<>();
    private final Map<ByteArrayWrapper /*Key*/, byte[] /*data*/> keyDataCache = new ConcurrentHashMap<>();

    @Getter
    private int numLeaves = 0;
    @Getter
    private int depth = 0;
    private byte[] rootHash = null;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean hasUnsavedChanges = new AtomicBoolean(false);
    private AtomicBoolean trackTimeOfOperations = new AtomicBoolean(false);
    
    /**
     * Lock for reading/writing to the tree.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //endregion

    //region ===================== Constructors =====================
    public MerkleTree(String treeName) throws RocksDBException {
        ObjectsInMemoryTracker.trackObject(this);
        RocksDB.loadLibrary();
        this.treeName = treeName;
        errorIf(openTrees.containsKey(treeName), "There is already open instance of this tree");

        // 1. Ensure directory exists
        path = "merkleTree/" + treeName;
        File directory = new File(path);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new RocksDBException("Failed to create directory: " + path);
        }

        // 2. Initialize DB
        initializeDb();

        // 7. Load initial metadata
        loadMetaData();

        // 8. Register instance
        openTrees.put(treeName, this);

        // 9. Force manual compaction on startup to reduce memory footprint
        try {
            db.compactRange();
        } catch (Exception e) {
            // Ignore compaction errors
        }
    }

    public MerkleTree(String treeName, boolean trackTimeOfOperations) throws RocksDBException {
        this(treeName);
        this.trackTimeOfOperations.set(trackTimeOfOperations);
    }
    //endregion

    //region ===================== Public Methods =====================

    /**
     * Get the current root hash of the Merkle tree.
     */
    public byte[] getRootHash() {
        errorIfClosed();
        getReadLock();
        try {
            if (rootHash == null) return null;
            else return Arrays.copyOf(rootHash, rootHash.length);
        } finally {
            releaseReadLock();
        }
    }

    public byte[] getRootHashSavedOnDisk() {
        errorIfClosed();
        getReadLock();
        try {
            return getData(metaDataHandle, KEY_ROOT_HASH.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            releaseReadLock();
        }
    }

    public int getNumLeaves() {
        errorIfClosed();
        getReadLock();
        try {
            return numLeaves;
        } finally {
            releaseReadLock();
        }
    }

    public int getDepth() {
        errorIfClosed();
        getReadLock();
        try {
            return depth;
        } finally {
            releaseReadLock();
        }
    }

    /**
     * Get all nodes saved on disk.
     *
     * @return A list of all nodes in the tree
     * @throws RocksDBException If there's an error accessing RocksDB
     */
    public HashSet<Node> getAllNodes() throws RocksDBException {
        errorIfClosed();
        getReadLock();
        try {
            HashSet<Node> allNodes = new HashSet<>();

            // First flush any pending changes to disk
            flushToDisk(false);

            ensureDbOpen();
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
            releaseReadLock();
        }
    }

    /**
     * Get data for a key from the Merkle Tree.
     *
     * @param key The key to retrieve data for
     * @return The data for the key, or null if the key doesn't exist
     * @throws RocksDBException         If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key is null
     */
    public byte[] getData(byte[] key) {
        errorIfClosed();
        byte[] data = keyDataCache.get(new ByteArrayWrapper(key));
        if (data != null) return data;

        long startTime = System.currentTimeMillis();
        try {
            return getData(keyDataHandle, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 2) System.out.println(treeName + " getData completed in " + (endTime - startTime) + " ms");
        }
    }

    /**
     * Add or update data for a key in the Merkle Tree.
     * This will create a new leaf node with a hash derived from the key and data,
     * or update an existing leaf if the key already exists.
     *
     * @param key  The key to store data for
     * @param data The data to store
     * @throws RocksDBException         If there's an error accessing RocksDB
     * @throws IllegalArgumentException If key or data is null
     */
    public void addOrUpdateData(byte[] key, byte[] data) throws RocksDBException {
        long startTime = System.currentTimeMillis();
        errorIfClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }

        getWriteLock();
        try {
            // Check if key already exists
            byte[] existingData = getData(key);
            byte[] oldLeafHash = existingData == null ? null : calculateLeafHash(key, existingData);

            // Calculate hash from key and data
            byte[] newLeafHash = calculateLeafHash(key, data);

            if (oldLeafHash != null && Arrays.equals(oldLeafHash, newLeafHash)) return;

            // Store key-data mapping
            keyDataCache.put(new ByteArrayWrapper(key), data);
            hasUnsavedChanges.set(true);

            if (oldLeafHash == null) {
                // Key doesn't exist, add new leaf
                addLeaf(new Node(newLeafHash));
            } else {
                // Key exists, update leaf
                // First get the old leaf hash
                updateLeaf(oldLeafHash, newLeafHash);
            }
        } finally {
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 1) System.out.println(treeName + " addOrUpdateData completed in " + (endTime - startTime) + " ms");
        }
    }

    public void revertUnsavedChanges() {
        if(!hasUnsavedChanges.get()) return;
        errorIfClosed();

        getWriteLock();
        try {
            nodesCache.clear();
            hangingNodes.clear();
            keyDataCache.clear();

            loadMetaData();

            hasUnsavedChanges.set(false);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            releaseWriteLock();
        }
    }

    public boolean containsKey(byte[] key) {
        errorIfClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        getReadLock();
        try {
            return getData(keyDataHandle, key) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } finally {
            releaseReadLock();
        }
    }

    public List<byte[]> getAllKeys() throws RocksDBException {
        errorIfClosed();
        getReadLock();
        try {
            ensureDbOpen();

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
            releaseReadLock();
        }
    }

    public List<byte[]> getAllData() throws RocksDBException {
        errorIfClosed();
        getReadLock();
        try {
            ensureDbOpen();

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
            releaseReadLock();
        }
    }

    public BiResult<List<byte[]>, List<byte[]>> keysAndTheirValues() throws RocksDBException {
        errorIfClosed();
        getReadLock();
        try {
            ensureDbOpen();

            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            try (RocksIterator iterator = db.newIterator(keyDataHandle)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    keys.add(iterator.key());
                    values.add(iterator.value());
                    iterator.next();
                }
            }
            return new BiResult<>(keys, values);
        } finally {
            releaseReadLock();
        }
    }

    /**
     * Flush all in-memory changes (nodes, metadata) to RocksDB.
     */
    public void flushToDisk(boolean releaseDb) throws RocksDBException {
        long startTime = System.currentTimeMillis();
        errorIfClosed();
        getWriteLock();
        try {
            if (hasUnsavedChanges.get()) {
                ensureDbOpen();

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
                    } else {
                        batch.delete(metaDataHandle, KEY_ROOT_HASH.getBytes());
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

                    for (Map.Entry<ByteArrayWrapper, byte[]> entry : keyDataCache.entrySet()) {
                        batch.put(keyDataHandle, entry.getKey().data(), entry.getValue());
                    }

                    try (WriteOptions writeOptions = new WriteOptions()) {
                        db.write(writeOptions, batch);
                    }

                    nodesCache.clear();
                    keyDataCache.clear();
                    hasUnsavedChanges.set(false);
                }
            }

            if(releaseDb) releaseDatabase();
        } finally {
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 1) System.out.println(treeName + " flush completed in " + (endTime - startTime) + " ms");
        }
    }

    /**
     * Close the databases (optional, if you need cleanup).
     */
    public void close() throws RocksDBException {
        long startTime = System.currentTimeMillis();
        getWriteLock();
        try {
            if (closed.get()) return;
            flushToDisk(true);
            openTrees.remove(treeName);
        } finally {
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 1) System.out.println(treeName + " closed in " + (endTime - startTime) + " ms");
        }
    }

    public MerkleTree clone(String newTreeName) throws RocksDBException, IOException {
        long startTime = System.currentTimeMillis();
        errorIfClosed();

        if (newTreeName == null || newTreeName.isEmpty()) {
            throw new IllegalArgumentException("New tree name cannot be null or empty");
        }

        if (openTrees.containsKey(newTreeName)) {
            MerkleTree existingTree = openTrees.get(newTreeName);
            existingTree.close();
        }

        File destDir = new File("merkleTree/" + newTreeName);

        if (destDir.exists()) {
            FileUtils.deleteDirectory(destDir);
        } else {
            // If the directory has sub-directories then create them without creating the directory itself
            File parent = destDir.getParentFile();
            if (parent != null && !parent.exists()) {
                if (!parent.mkdirs()) {
                    throw new IOException("Failed to create parent directories for " + destDir);
                }
            }
        }

        getWriteLock();
        try {
            flushToDisk(false);
            ensureDbOpen();

            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(destDir.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            return new MerkleTree(newTreeName);
        } finally {
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get()) System.out.println("Clone of " + newTreeName + " completed in " + (endTime - startTime) + " ms");
        }
    }

    public void update(MerkleTree sourceTree) throws RocksDBException, IOException {
        long startTime = System.currentTimeMillis();
        errorIfClosed();
        getWriteLock();
        sourceTree.getWriteLock();
        try {
            if (sourceTree == null) {
                throw new IllegalArgumentException("Source tree cannot be null");
            }

            byte[] rootHashSavedOnDisk = getRootHashSavedOnDisk();
            byte[] sourceRootHashSavedOnDisk = sourceTree.getRootHashSavedOnDisk();
            if (
                    (rootHashSavedOnDisk == null && sourceRootHashSavedOnDisk == null)
                            ||
                            (rootHashSavedOnDisk != null && sourceRootHashSavedOnDisk != null)
                                    && Arrays.equals(getRootHashSavedOnDisk(), sourceTree.getRootHashSavedOnDisk())
            ) {
                //This means that this tree is already a copy of the source tree and we only need to replace the cache
                copyCache(sourceTree);
            } else {
                if(metaDataHandle != null) {
                    metaDataHandle.close();
                    metaDataHandle = null;
                }
                if(nodesHandle != null) {
                    nodesHandle.close();
                    nodesHandle = null;
                }
                if(keyDataHandle != null) {
                    keyDataHandle.close();
                    keyDataHandle = null;
                }
                if(db != null && !db.isClosed()) {
                        db.close();
                        db = null;
                };

                sourceTree.flushToDisk(false);

                File thisTreesDirectory = new File(path);
                FileUtils.deleteDirectory(thisTreesDirectory);

                try (Checkpoint checkpoint = Checkpoint.create(sourceTree.db)) {
                    checkpoint.createCheckpoint(thisTreesDirectory.getAbsolutePath());
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                // Reinitialize the database
                initializeDb();
                loadMetaData();

                nodesCache.clear();
                keyDataCache.clear();
                hangingNodes.clear();
                hasUnsavedChanges.set(false);
            }

        } finally {
            sourceTree.releaseWriteLock();
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 1) System.out.println(treeName + " update completed in " + (endTime - startTime) + " ms");
        }
    }

    /**
     * Efficiently clears the entire MerkleTree by closing, deleting and recreating the RocksDB instance.
     * This is much faster than iterating through all entries and deleting them individually.
     */
    public void clear() throws RocksDBException {
        long startTime = System.currentTimeMillis();
        errorIfClosed();
        getWriteLock();
        try {
            ensureDbOpen();

            // mark every key in each CF as deleted (empty → 0xFF)
            byte[] start = new byte[0];
            byte[] end   = new byte[]{(byte)0xFF};

            // these three calls are each O(1)
            db.deleteRange(metaDataHandle, start, end);
            db.deleteRange(nodesHandle,    start, end);
            db.deleteRange(keyDataHandle,  start, end);

            // OPTIONAL: reclaim space immediately
            db.compactRange(metaDataHandle);
            db.compactRange(nodesHandle);
            db.compactRange(keyDataHandle);

            // reset your in-memory state
            nodesCache.clear();
            keyDataCache.clear();
            hangingNodes.clear();
            rootHash  = null;
            numLeaves = depth = 0;
            hasUnsavedChanges.set(false);

        } finally {
            releaseWriteLock();
            long endTime = System.currentTimeMillis();
            if(trackTimeOfOperations.get() && endTime - startTime > 1) System.out.println(treeName + " cleared in " + (endTime - startTime) + " ms");
        }
    }

    public JSONObject getRamInfo() {
        JSONObject json = new JSONObject();
        json.put("treeName", treeName);
        json.put("numLeaves", numLeaves);
        json.put("depth", depth);
        json.put("nodeCacheSize", nodesCache.size());
        json.put("keyDataCacheSize", keyDataCache.size());
        json.put("hangingNodesCacheSize", hangingNodes.size());
        return json;
    }

    /**
     * Gracefully closes the underlying RocksDB instance and its associated column family handles,
     * without destroying the MerkleTree object itself.
     * <p>
     * This method is intended to free up system resources (such as file handles, memory, and background threads)
     * for trees that are temporarily inactive. The MerkleTree instance remains usable, and the database
     * can be reopened later when needed.
     * </p>
     *
     * <p>Calling this method multiple times is safe — it will have no effect if the database is already closed.</p>
     *
     * <p><b>Important:</b> The caller must ensure that all necessary data has been flushed to disk
     * before invoking this method (e.g., via {@code flushToDisk()}).</p>
     */
    public void releaseDatabase() {
        getWriteLock();
        try {
            if(closed.get()) return;

            if (metaDataHandle != null) {
                try {
                    metaDataHandle.close();
                    metaDataHandle = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (nodesHandle != null) {
                try {
                    nodesHandle.close();
                    nodesHandle = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (keyDataHandle != null) {
                try {
                    keyDataHandle.close();
                    keyDataHandle = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (db != null) {
                try {
                    db.close();
                    db = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            closed.set(true);
        } finally {
            releaseWriteLock();
        }
    }

    //endregion

    //region ===================== Private Methods =====================
    private void getWriteLock() {
        long startTime = System.currentTimeMillis();
        lock.writeLock().lock();
        long endTime = System.currentTimeMillis();
        
        if(trackTimeOfOperations.get() && endTime - startTime >= 2) {
            System.out.println(treeName + " getWriteLock unexpectedly took " + (endTime - startTime) + " ms");
        }
    }
    
    private void releaseWriteLock() {
        long startTime = System.currentTimeMillis();
        lock.writeLock().unlock();
        long endTime = System.currentTimeMillis();
        
        if(trackTimeOfOperations.get() && endTime - startTime >= 2) {
            System.out.println(treeName + " releaseWriteLock unexpectedly took " + (endTime - startTime) + " ms");
        }
    }
    
    private void getReadLock() {
        long startTime = System.currentTimeMillis();
        lock.readLock().lock();
        long endTime = System.currentTimeMillis();
        
        if(trackTimeOfOperations.get() && endTime - startTime >= 2) {
            System.out.println(treeName + " getReadLock unexpectedly took " + (endTime - startTime) + " ms");
        }
    }
    
    private void releaseReadLock() {
        long startTime = System.currentTimeMillis();
        lock.readLock().unlock();
        long endTime = System.currentTimeMillis();
        
        if(trackTimeOfOperations.get() && endTime - startTime >= 2) {
            System.out.println(treeName + " releaseReadLock unexpectedly took " + (endTime - startTime) + " ms");
        }
    }
    
    private void initializeDb() throws RocksDBException {
        // 1) DBOptions
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setUseDirectReads(true)
                .setAllowMmapReads(false)
                .setUseDirectIoForFlushAndCompaction(true)
                .setMaxOpenFiles(100)
                .setMaxBackgroundJobs(1)
                .setInfoLogLevel(InfoLogLevel.FATAL_LEVEL)
                .setMaxManifestFileSize(64L * 1024 * 1024)  // e.g. 64 MB
                .setMaxTotalWalSize(250L * 1024 * 1024)  // total WAL across all CFs ≤ 250 MB
                .setWalSizeLimitMB(250)                 // (optional) per-WAL-file soft limit
                .setKeepLogFileNum(3);    // keep at most 3 WAL files, regardless of age/size

// 2) Table format: no cache, small blocks
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setNoBlockCache(true)
                .setBlockSize(4 * 1024)        // 4 KB blocks
                .setFormatVersion(5)
                .setChecksumType(ChecksumType.kxxHash);

// 3) ColumnFamilyOptions: no compression, single write buffer
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setTableFormatConfig(tableConfig)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setWriteBufferSize(16 * 1024 * 1024)  // 16 MB memtable
                .setMaxWriteBufferNumber(1)
                .setMinWriteBufferNumberToMerge(1)
                .optimizeUniversalStyleCompaction();

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
        
        closed.set(false);
    }

    private synchronized void ensureDbOpen() throws RocksDBException {
        if (closed.get()) initializeDb();
    }


    private byte[] getData(ColumnFamilyHandle handle, byte[] key) throws RocksDBException {
        getReadLock();
        try {
            ensureDbOpen();
            
            return db.get(handle, key);
        } finally {
            releaseReadLock();
        }
    }

    /**
     * Load the tree's metadata from RocksDB.
     */
    private void loadMetaData() throws RocksDBException {
        getReadLock();
        try {
            this.rootHash = getData(metaDataHandle, KEY_ROOT_HASH.getBytes());

            byte[] numLeavesBytes = getData(metaDataHandle, KEY_NUM_LEAVES.getBytes());
            this.numLeaves = (numLeavesBytes == null)
                    ? 0
                    : ByteBuffer.wrap(numLeavesBytes).getInt();

            byte[] depthBytes = getData(metaDataHandle, KEY_DEPTH.getBytes());
            this.depth = (depthBytes == null)
                    ? 0
                    : ByteBuffer.wrap(depthBytes).getInt();

            // Load any hangingNodes from metadata
            hangingNodes.clear();
            for (int i = 0; i <= depth; i++) {
                byte[] hash = getData(metaDataHandle, (KEY_HANGING_NODE_PREFIX + i).getBytes());
                if (hash != null) {
                    Node node = getNodeByHash(hash);
                    hangingNodes.put(i, node.hash);
                }
            }
        } finally {
            releaseReadLock();
        }
    }

    /**
     * Fetch a node by its hash, either from the in-memory cache or from RocksDB.
     */
    private Node getNodeByHash(byte[] hash) throws RocksDBException {
        getReadLock();
        try {
            if (hash == null) return null;

            ByteArrayWrapper baw = new ByteArrayWrapper(hash);
            Node node = nodesCache.get(baw);
            if (node == null) {
                try {
                    byte[] encodedData = getData(nodesHandle, hash);
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
            releaseReadLock();
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

        if (hasLeft) buffer.get(left);
        if (hasRight) buffer.get(right);
        if (hasParent) buffer.get(parent);

        return new Node(hash, left, right, parent);
    }

    private byte[] calculateLeafHash(byte[] key, byte[] data) {
        return PWRHash.hash256(key, data);
    }

    /**
     * Add a new leaf node to the Merkle Tree.
     */
    private void addLeaf(Node leafNode) throws RocksDBException {
        if (leafNode == null) {
            throw new IllegalArgumentException("Leaf node cannot be null");
        }
        if (leafNode.hash == null) {
            throw new IllegalArgumentException("Leaf node hash cannot be null");
        }

        getWriteLock();
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
            releaseWriteLock();
        }
    }

    private void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
        if (oldLeafHash == null) {
            throw new IllegalArgumentException("Old leaf hash cannot be null");
        }
        if (newLeafHash == null) {
            throw new IllegalArgumentException("New leaf hash cannot be null");
        }
        errorIf(Arrays.equals(oldLeafHash, newLeafHash), "Old and new leaf hashes cannot be the same");

        getWriteLock();
        try {
            Node leaf = getNodeByHash(oldLeafHash);

            if (leaf == null) {
                throw new IllegalArgumentException("Leaf not found: " + Hex.toHexString(oldLeafHash));
            } else {
                leaf.updateNodeHash(newLeafHash);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Error updating leaf: " + e.getMessage(), e);
        } finally {
            releaseWriteLock();
        }
    }

    /**
     * Add a node at a given level.
     */
    private void addNode(int level, Node node) throws RocksDBException {
        getWriteLock();
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
            } else if (hangingNode.parent == null) {
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
            releaseWriteLock();
        }
    }

    private void errorIfClosed() {
        if (closed.get()) {
            throw new IllegalStateException("MerkleTree is closed");
        }
    }

    private void copyCache(MerkleTree sourceTree) {
        nodesCache.clear();
        keyDataCache.clear();
        hangingNodes.clear();

        for (Map.Entry<ByteArrayWrapper, Node> entry : sourceTree.nodesCache.entrySet()) {
            nodesCache.put(entry.getKey(), new Node(entry.getValue()));
        }

        for (Map.Entry<ByteArrayWrapper, byte[]> entry : sourceTree.keyDataCache.entrySet()) {
            keyDataCache.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }

        for (Map.Entry<Integer, byte[]> entry : sourceTree.hangingNodes.entrySet()) {
            hangingNodes.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }

        rootHash = (sourceTree.rootHash != null) ? Arrays.copyOf(sourceTree.rootHash, sourceTree.rootHash.length) : null;
        numLeaves = sourceTree.numLeaves;
        depth = sourceTree.depth;
        hasUnsavedChanges.set(sourceTree.hasUnsavedChanges.get());
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
         * The old hash of the node before it was updated. This is used to delete the old node from the db.
         */
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
         * Copy constructor for Node.
         */
        public Node(Node node) {
            this.hash = Arrays.copyOf(node.hash, node.hash.length);
            this.left = (node.left != null) ? Arrays.copyOf(node.left, node.left.length) : null;
            this.right = (node.right != null) ? Arrays.copyOf(node.right, node.right.length) : null;
            this.parent = (node.parent != null) ? Arrays.copyOf(node.parent, node.parent.length) : null;
            this.nodeHashToRemoveFromDb = (node.nodeHashToRemoveFromDb != null) ? Arrays.copyOf(node.nodeHashToRemoveFromDb, node.nodeHashToRemoveFromDb.length) : null;
        }

        /**
         * Calculate the hash of this node based on the left and right child hashes.
         */
        public byte[] calculateHash() {
            getReadLock();
            try {
                if (left == null && right == null) {
                    // Could be a leaf that hasn't set its hash, or zero-length node
                    return null;
                }

                byte[] leftHash = (left != null) ? left : right; // Duplicate if necessary
                byte[] rightHash = (right != null) ? right : left;
                return PWRHash.hash256(leftHash, rightHash);
            } finally {
                releaseReadLock();
            }
        }

        /**
         * Encode the node into a byte[] for storage in RocksDB.
         */
        public byte[] encode() {
            getReadLock();
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
                releaseReadLock();
            }
        }

        /**
         * Set this node's parent.
         */
        public void setParentNodeHash(byte[] parentHash) {
            getWriteLock();
            try {
                this.parent = parentHash;
            } finally {
                releaseWriteLock();
            }
        }

        /**
         * Update this node's hash and propagate the change upward.
         */
        public void updateNodeHash(byte[] newHash) throws RocksDBException {
            getWriteLock();
            try {
                //We only store the old hash if it is not already set. Because we only need to delete the old node from the db once.
                //New hashes don't have copies in db since they haven't been flushed to disk yet
                if (nodeHashToRemoveFromDb == null) nodeHashToRemoveFromDb = this.hash;

                byte[] oldHash = Arrays.copyOf(this.hash, this.hash.length);
                this.hash = newHash;

                for (Map.Entry<Integer, byte[]> entry : hangingNodes.entrySet()) {
                    if (Arrays.equals(entry.getValue(), oldHash)) {
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
                releaseWriteLock();
            }
        }

        /**
         * Update a leaf (left or right) if it matches the old hash.
         */
        public void updateLeaf(byte[] oldLeafHash, byte[] newLeafHash) {
            getWriteLock();
            try {
                if (left != null && Arrays.equals(left, oldLeafHash)) {
                    left = newLeafHash;
                } else if (right != null && Arrays.equals(right, oldLeafHash)) {
                    right = newLeafHash;
                } else {
                    throw new IllegalArgumentException("Old hash not found among this node's children");
                }
            } finally {
                releaseWriteLock();
            }
        }

        /**
         * Add a leaf to this node (either left or right).
         */
        public void addLeaf(byte[] leafHash) throws RocksDBException {
            if (leafHash == null) {
                throw new IllegalArgumentException("Leaf hash cannot be null");
            }

            getWriteLock();
            try {
                Node leafNode = getNodeByHash(leafHash);
                if (leafNode == null) {
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
                releaseWriteLock();
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
            if (this.hash == null && other.hash != null) {
                return false;
            } else if (this.hash != null && other.hash == null) {
                return false;
            } else if (this.hash != null && other.hash != null) {
                if (!Arrays.equals(this.hash, other.hash)) {
                    return false;
                }
            }

            // Compare left child reference
            if (this.left == null && other.left != null) {
                return false;
            } else if (this.left != null && other.left == null) {
                return false;
            } else if (this.left != null && other.left != null) {
                if (!Arrays.equals(this.left, other.left)) {
                    return false;
                }
            }

            // Compare right child reference
            if (this.right == null && other.right != null) {
                return false;
            } else if (this.right != null && other.right == null) {
                return false;
            } else if (this.right != null && other.right != null) {
                if (!Arrays.equals(this.right, other.right)) {
                    return false;
                }
            }

            if (this.parent == null && other.parent != null) {
                return false;
            } else if (this.parent != null && other.parent == null) {
                return false;
            } else if (this.parent != null && other.parent != null) {
                if (!Arrays.equals(this.parent, other.parent)) {
                    return false;
                }
            }

            // Note: We don't compare parent references as they can legitimately differ
            // between source and cloned trees while still representing the same logical node

            return true;
        }
    }
    //endregion

    public static void main(String[] args) throws Exception {
        MerkleTree tree = new MerkleTree("w1e21191we3/tree1");
        tree.addOrUpdateData("key1".getBytes(), "value1".getBytes());

        MerkleTree tree2 = tree.clone("we9211131we/tree2");

        tree.addOrUpdateData("key2".getBytes(), "value2".getBytes());
        tree.flushToDisk(true);

        System.out.println("u");
        tree2.update(tree);
        System.out.println("ud");

        tree.flushToDisk(true);
        tree2.flushToDisk(true);

        //compare all keys and values of both trees
        List<byte[]> keys1 = tree.getAllKeys();
        List<byte[]> keys2 = tree2.getAllKeys();

        List<byte[]> values1 = tree.getAllData();
        List<byte[]> values2 = tree2.getAllData();

        if(keys1.size() != keys2.size()) {
            System.out.println("Keys size do not match: " + keys1.size() + " != " + keys2.size());
        } else {
            System.out.println("Keys size match: " + keys1.size());
        }

        if(values1.size() != values2.size()) {
            System.out.println("Values size do not match: " + values1.size() + " != " + values2.size());
        } else {
            System.out.println("Values size match: " + values1.size());
        }

        for (int i = 0; i < keys1.size(); i++) {
            byte[] key1 = keys1.get(i);
            byte[] value1 = values1.get(i);

            byte[] key2 = keys2.get(i);
            byte[] value2 = values2.get(i);

            if (!Arrays.equals(key1, key2)) {
                System.out.println("Keys do not match: " + Hex.toHexString(key1) + " != " + Hex.toHexString(key2));
            } else {
                System.out.println("Keys match: " + new String(key1));
            }

            if (!Arrays.equals(value1, value2)) {
                System.out.println("Values do not match: " + Hex.toHexString(value1) + " != " + Hex.toHexString(value2));
            } else {
                System.out.println("Values match: " + new String(value1));
            }
        }
    }
}
