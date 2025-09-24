package io.pwrlabs.utils;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static io.pwrlabs.newerror.NewError.errorIf;

/**
 * A utility class that manages named {@link PWRReentrantReadWriteLock} instances.
 *
 * <p>This manager provides a centralized registry for named read-write locks, allowing
 * different parts of an application to access the same lock by name. This enables
 * coordinated access to shared resources across multiple components without directly
 * sharing lock references.</p>
 *
 * <p>The manager uses a thread-safe map implementation to store locks, ensuring
 * that lock creation and retrieval operations are thread-safe.</p>
 *
 * <p>Usage example:</p>
 * <pre>
 * // Create a named lock
 * PWRReentrantReadWriteLock userDataLock = PWRReentrantReadWriteLockManager.createReadWriteLock("userData");
 *
 * // Later, in another component
 * Map&lt;String, PWRReentrantReadWriteLock&gt; locks = manager.getReadWriteLocksCopy();
 * PWRReentrantReadWriteLock sameUserDataLock = locks.get("userData");
 * </pre>
 */
public class PWRReentrantReadWriteLockManager {

    /**
     * Thread-safe map storing all managed read-write locks, keyed by their names.
     * ConcurrentHashMap is used to ensure thread safety for concurrent access.
     */
    private static Map<String /*Lock Name*/, WeakReference<PWRReentrantReadWriteLock>> readWriteLocks = new ConcurrentHashMap<>();

    /**
     * Creates and registers a new {@link PWRReentrantReadWriteLock} with the specified name.
     *
     * <p>This method is synchronized to ensure that only one lock can be created with
     * a given name, preventing race conditions during lock creation.</p>
     *
     * @param name The unique name to identify this lock in the manager
     * @return The newly created read-write lock
     * @throws RuntimeException if a lock with the specified name already exists,
     *         with the message "Lock with name [name] already exists"
     */
    public static synchronized PWRReentrantReadWriteLock createReadWriteLock(String name, long unhealthyWriteLockAcquireMs) {
        WeakReference<PWRReentrantReadWriteLock> ref = readWriteLocks.get(name);
        PWRReentrantReadWriteLock existingLock = (ref != null) ? ref.get() : null;

        errorIf(existingLock != null, "Lock with name " + name + " already exists");

        // If we get here, either no entry exists or the lock was garbage collected
        if (ref != null && ref.get() == null) {
            // Clean up the stale reference
            readWriteLocks.remove(name);
        }

        PWRReentrantReadWriteLock lock = new PWRReentrantReadWriteLock(name, unhealthyWriteLockAcquireMs);
        readWriteLocks.put(name, new WeakReference<>(lock));

        return lock;
    }

    /**
     * Returns a copy of the internal map of all managed read-write locks.
     *
     * <p>The returned map is a new instance that contains all the current locks, but
     * modifications to the returned map do not affect the locks managed by this class.
     * The lock references themselves are shared, not copied.</p>
     *
     * @return A new ConcurrentHashMap containing all the currently managed locks, keyed by their names
     */
    public static Map<String, PWRReentrantReadWriteLock> getReadWriteLocksCopy() {
        Map<String, PWRReentrantReadWriteLock> result = new ConcurrentHashMap<>();

        // Copy only locks that are still alive
        for (Map.Entry<String, WeakReference<PWRReentrantReadWriteLock>> entry : readWriteLocks.entrySet()) {
            PWRReentrantReadWriteLock lock = entry.getValue().get();
            if (lock != null) {
                result.put(entry.getKey(), lock);
            } else {
                // Clean up stale references
                readWriteLocks.remove(entry.getKey());
            }
        }

        return result;
    }
}