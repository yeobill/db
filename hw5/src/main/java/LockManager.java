import com.sun.org.apache.regexp.internal.RE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {
        if (! this.tableToTableLock.containsKey(tableName)) {
            this.tableToTableLock.put(tableName, new TableLock(lockType));
        }
        TableLock tableLock = this.tableToTableLock.get(tableName);

        // Coner case1
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }
        // if curr Lock is empty then add transaction
        if (tableLock.lockOwners.isEmpty()) {
            tableLock.lockOwners.add(transaction);
        } else {
            // Coner case2
            if (tableLock.lockType == LockType.Exclusive && lockType == LockType.Shared) {
                if (tableLock.lockOwners.iterator().next().getName().equals(transaction.getName())){
                    throw new IllegalArgumentException();
                }
                else {

                    if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                        this.waitDie(tableName, transaction, lockType);
                    } else if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WoundWait)) {
                        this.woundWait(tableName, transaction, lockType);
                    } else {
                        Request lock = new Request(transaction, lockType);
//                    if (transaction.getTimestamp() > tableLock.lockOwners.iterator().next().getTimestamp()) {
//
//                    }
                        lock.transaction.setStatus(Transaction.Status.Waiting);
                        tableLock.requestersQueue.add(lock);
                    }
                }
            }
            // Coner case3
            else if (tableLock.lockOwners.contains(transaction) && tableLock.lockType == lockType) {
                throw new IllegalArgumentException();
            }
            // if locktype is exclusive then the lock request is denied
            else if (tableLock.lockType == LockType.Exclusive) {
                if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                    this.waitDie(tableName, transaction, lockType);
                } else if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WoundWait)) {
                    this.woundWait(tableName, transaction, lockType);
                } else {
                    Request lock = new Request(transaction, lockType);
                    lock.transaction.setStatus(Transaction.Status.Waiting);
                    tableLock.requestersQueue.add(lock);
                }
            }
            // if transaction is in lockOwners with shared type then switch the Locktype
            else if (tableLock.lockType == LockType.Shared && tableLock.lockOwners.size() == 1
                    && tableLock.lockOwners.contains(transaction)) {
                if (lockType.equals( LockType.Exclusive)) {
                    tableLock.lockType = lockType;
                }
            }
            // if locktype is shared and a request type is exclusive then it is denied
            else if (tableLock.lockType == LockType.Shared && lockType == LockType.Exclusive) {
                if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                    this.waitDie(tableName, transaction, lockType);
                }else if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WoundWait)) {
                    this.woundWait(tableName, transaction, lockType);
                }  else {
                    Request lock = new Request(transaction, lockType);
                    lock.transaction.setStatus(Transaction.Status.Waiting);
                    tableLock.requestersQueue.add(lock);
                }

            } else if (tableLock.lockType == LockType.Shared && lockType == LockType.Shared) {
                if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                    this.waitDie(tableName, transaction, lockType);
                } else if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WoundWait)) {
                    this.woundWait(tableName, transaction, lockType);
                } else {
                    transaction.setStatus(Transaction.Status.Running);
                    tableLock.lockOwners.add(transaction);
                }
            }
        }
    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        return false;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{
        // get the tableLock. if not in tableToTableLock then thorws exception
        if (! this.tableToTableLock.containsKey(tableName)) {
            throw new IllegalArgumentException();
        }
        // Coner case1
        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }
        TableLock tableLock = this.tableToTableLock.get(tableName);

        if (tableLock.lockOwners.isEmpty()) {
            if (!tableLock.requestersQueue.isEmpty()) {
                if (tableLock.requestersQueue.getFirst().lockType == LockType.Shared) {
                    LinkedList<Request> rmLock = new LinkedList<>();
                    for (Request lock :tableLock.requestersQueue) {
                        if (lock.lockType == LockType.Shared) {
                            lock.transaction.setStatus(Transaction.Status.Running);
                            tableLock.lockOwners.add(lock.transaction);
                            rmLock.add(lock);
                        }
                    }
                    for (Request lock : rmLock) {
                        tableLock.requestersQueue.remove(lock);
                    }
                    tableLock.lockOwners.remove(transaction);
                } else {
                    Request lock = tableLock.requestersQueue.getFirst();
                    tableLock.requestersQueue.remove(lock);
                    lock.transaction.setStatus(Transaction.Status.Running);
                    tableLock.lockOwners.add(lock.transaction);
                    tableLock.lockOwners.remove(transaction);
                }
            } else {
                this.tableToTableLock.remove(tableName);
            }

        } else {
            // Coner case2
            if (! tableLock.lockOwners.contains(transaction)) {
                throw new IllegalArgumentException();
            } else {
                tableLock.lockOwners.remove(transaction);
            }
            if (tableLock.lockType == LockType.Shared) {
                LinkedList<Request> rmLock = new LinkedList<>();
                for (Request lock :tableLock.requestersQueue) {
                    if (lock.lockType == LockType.Shared) {
                        lock.transaction.setStatus(Transaction.Status.Running);
                        tableLock.lockOwners.add(lock.transaction);
                        rmLock.add(lock);
                    }
                }
                for (Request lock : rmLock) {
                    tableLock.requestersQueue.remove(lock);
                }
//                if (tableLock.lockOwners.size() == 1) {
//                    for (Request lock : )
//                    tableLock.lockOwners.iterator().next().getName()
//                }
            } if (tableLock.lockOwners.isEmpty()) {
                if (!tableLock.requestersQueue.isEmpty()) {
                    Request lock = tableLock.requestersQueue.getFirst();
                    tableLock.requestersQueue.remove(lock);
                    lock.transaction.setStatus(Transaction.Status.Running);
                    tableLock.lockOwners.remove(transaction);
                    tableLock.lockOwners.add(lock.transaction);
                    tableLock.lockType = lock.lockType;

                }

            }
        }
        // case1
        if (tableLock.lockOwners.size() == 1 && tableLock.lockType == LockType.Shared) {

//            Request lock = new Request(transaction, LockType.Exclusive);
            for (Request lock : tableLock.requestersQueue) {
                if (tableLock.lockOwners.iterator().next().getName().equals(lock.transaction.getName())) {
                    tableLock.lockOwners.remove(transaction);
                    tableLock.lockType = LockType.Exclusive;
                    tableLock.requestersQueue.remove(lock);
                    lock.transaction.setStatus(Transaction.Status.Running);
                    tableLock.lockOwners.add(lock.transaction);
                }
            }
            if (tableLock.lockType == LockType.Shared) {
                LinkedList<Request> rmLock = new LinkedList<>();

                for (Request lock :tableLock.requestersQueue) {
                    if (lock.lockType == LockType.Shared) {
                        lock.transaction.setStatus(Transaction.Status.Running);
                        tableLock.lockOwners.add(lock.transaction);
                        rmLock.add(lock);
                    }
                }
                for (Request lock : rmLock) {
                    tableLock.requestersQueue.remove(lock);
                }
            }
        }

    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        //TODO: HW5 Implement
        if (this.tableToTableLock.containsKey(tableName)) {
            return this.tableToTableLock.get(tableName).lockType == lockType
                    && this.tableToTableLock.get(tableName).lockOwners.contains(transaction);
        } else {
            return false;
        }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tableLock = this.tableToTableLock.get(tableName);
        // lower priority
        if (tableLock.lockType == LockType.Shared) {
            Boolean higher = false;
            for (Transaction holdLock : tableLock.lockOwners) {
                if (holdLock.getTimestamp() > transaction.getTimestamp()) {
                    higher = true;
                }
            }
            if (higher) {
                Request lock = new Request(transaction, lockType);
                lock.transaction.setStatus(Transaction.Status.Waiting);
                tableLock.requestersQueue.add(lock);
            } else {
                transaction.setStatus(Transaction.Status.Aborting);
            }
        }
        if (transaction.getTimestamp() > tableLock.lockOwners.iterator().next().getTimestamp()) {
            transaction.setStatus(Transaction.Status.Aborting);
        } else {
            Request lock = new Request(transaction, lockType);
            lock.transaction.setStatus(Transaction.Status.Waiting);
            tableLock.requestersQueue.add(lock);
        }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tableLock = this.tableToTableLock.get(tableName);
        // lower priority
        if (tableLock.lockType == LockType.Shared) {
            Boolean higher = false;
            int min = Integer.MAX_VALUE;
            for (Transaction holdLock : tableLock.lockOwners) {
                if (min > holdLock.getTimestamp()) {
                    min = holdLock.getTimestamp();
                }
            }
            if (min > transaction.getTimestamp()) {
                higher = true;
            }
            if (lockType == LockType.Shared) {
                transaction.setStatus(Transaction.Status.Running);
                tableLock.lockOwners.add(transaction);
            } else {
                if (higher) {
                    for (Transaction holdLock : tableLock.lockOwners) {
                        holdLock.setStatus(Transaction.Status.Aborting);
                        for (Request reqLock : tableLock.requestersQueue) {
                            if (reqLock.transaction == holdLock) {
                                reqLock.transaction.setStatus(Transaction.Status.Aborting);
                                tableLock.requestersQueue.remove(reqLock);
                            }
                        }
                    }
                    tableLock.lockOwners.clear();
                    transaction.setStatus(Transaction.Status.Running);
                    tableLock.lockOwners.add(transaction);
                    tableLock.lockType = lockType;
                } else {
                    Request lock = new Request(transaction, lockType);
                    lock.transaction.setStatus(Transaction.Status.Waiting);
                    tableLock.requestersQueue.add(lock);
                }
            }

        } else {
            if (transaction.getTimestamp() < tableLock.lockOwners.iterator().next().getTimestamp()) {
                Transaction holdLock = tableLock.lockOwners.iterator().next();
                tableLock.lockOwners.iterator().next().setStatus(Transaction.Status.Aborting);
                tableLock.lockOwners.clear();
                transaction.setStatus(Transaction.Status.Running);
                tableLock.lockOwners.add(transaction);
                tableLock.lockType = lockType;
                for (Request reqLock : tableLock.requestersQueue) {
                    if (reqLock.transaction == holdLock) {
                        reqLock.transaction.setStatus(Transaction.Status.Aborting);
                        tableLock.requestersQueue.remove(reqLock);
                    }
                }

            } else {
                Request lock = new Request(transaction, lockType);
                lock.transaction.setStatus(Transaction.Status.Waiting);
                tableLock.requestersQueue.add(lock);
            }
        }
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<Transaction>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
