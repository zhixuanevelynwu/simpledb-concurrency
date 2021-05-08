package simpledb;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;


/**
 * Keyholder manages which transactions gets access to a S or X lock
 */
public class KeyHolder {
	/*
	 * A help class that stores information of lock holder and permission
	 */
	public static class Locks {
		private TransactionId tid;
		private Permissions perm;

		public Locks(TransactionId t, Permissions perm) {
			this.tid = t;
			this.perm = perm;
		}
	}
	
	private static class NodeInfo {
		
	}

	// stores information the transactions that are accessing each page
	// -> page pid is locked by which locks
	private ConcurrentHashMap<PageId, ArrayList<Locks>> locking;
	// a waiting table
	private ConcurrentHashMap<TransactionId, PageId> dpGraph;
	// graph information
	private ConcurrentHashMap<TransactionId, NodeInfo> graphInfo;

	Permissions S = Permissions.READ_ONLY;
	Permissions X = Permissions.READ_WRITE;

	public KeyHolder() {
		this.locking = new ConcurrentHashMap<>();
		this.dpGraph = new ConcurrentHashMap<>();
	}

	/**
	 * lock a transaction
	 * 
	 * @param tid
	 * @param pid
	 * @param perm
	 * @return
	 * @throws InterruptedException
	 */
	public synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) throws InterruptedException {
		// both S and X locks avoid waiting-on-myself situation
		boolean locked = (perm == S) ? SLock(tid, pid) : XLock(tid, pid);
		while (!locked) {
			Thread.onSpinWait();
			// try to lock again
			locked = (perm == S) ? SLock(tid, pid) : XLock(tid, pid);
		}
		//locking.get(pid).add(new Locks(tid, perm)); // handled by lockHelper
		return locked;
	}

	/**
	 * unlock(release) a transaction
	 * 
	 * @param tid
	 * @param pid
	 * @return
	 */
	public synchronized boolean unlock(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		// no such lock
		if (lockedBy == null || lockedBy.size() == 0)
			return false;
		// find the lock -> unlock
		for (Locks l : lockedBy) {
			if (l.tid.equals(tid)) {
				lockedBy.remove(l);
				return true; // unlocked
			}
		}
		return false;
	}

	/**
	 * grant S lock on page pid to transaction tid
	 * 
	 * @param tid
	 * @param pid
	 * @return true if granted
	 */
	public synchronized boolean SLock(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		if (lockedBy != null && lockedBy.size() > 0) {
			// single transaction locking
			if (lockedBy.size() == 1) {
				Locks l = lockedBy.get(0);
				// is it my lock?
				if (l.tid.equals(tid)) {
					return (l.perm == S) ? true : lockHelper(pid, tid, S);
				} else {
					if (l.perm == S)
						return lockHelper(pid, tid, S);
					else {
						dpGraph.put(tid, pid);						
						return false;
					}
				}
			} else {
				// multiple transactions locking
				for (Locks l : lockedBy) {
					if (l.perm == X) { // exists an X lock
						if (l.tid.equals(tid))
							return true;
						else {
							dpGraph.put(tid, pid);
							return false;
						}
					} else if (l.tid.equals(tid)) {
						return true; // my S lock is already there
					}
				}
				// multiple S locks -> fine to grant a lock
				return lockHelper(pid, tid, S);
			}
		} else {
			// nothing in the list -> no conflicts
			return lockHelper(pid, tid, S);
		}
	}

	/**
	 * grant X lock on page pid to transaction tid
	 * 
	 * @param tid
	 * @param pid
	 * @return
	 */
	public synchronized boolean XLock(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		if (lockedBy != null && lockedBy.size() > 0) {
			// only one or two locks -> mine or not mine
			if (lockedBy.size() == 1) {
				Locks l = lockedBy.get(0);
				// is it my lock?
				if (l.tid.equals(tid)) {
					return (l.perm == X) ? true : lockHelper(pid, tid, X);
				} else
					return false;
			} else if (lockedBy.size() == 2) {
				for (Locks l : lockedBy) {
					if (l.tid.equals(tid) && l.perm == X) {
						return true;
					}
				}
				//addWait(tid, pid);
				return false;
			}
			// beyond 2 locks -> impossible to grant permission
			//addWait(tid, pid);
			return false;
		} else {
			// nothing in the list -> no conflicts
			return lockHelper(pid, tid, X);
		}
	}

	/**
	 * @param pid
	 * @param tid
	 * @param perm
	 * @return
	 */
	private synchronized boolean lockHelper(PageId pid, TransactionId tid, Permissions perm) {
		Locks mylock = new Locks(tid, perm);
		ArrayList<Locks> lockedBy = locking.get(pid);
		if (lockedBy == null || lockedBy.size() == 0)
			lockedBy = new ArrayList<>();
		lockedBy.add(mylock);
		locking.put(pid, lockedBy);
		dpGraph.remove(tid);
		return true;
	}

	/**
	 * check if a lock on a page exists
	 * 
	 * @param tid
	 * @param pid
	 * @return
	 */
	public boolean exists(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		if (lockedBy == null || lockedBy.size() == 0)
			return false;
		for (Locks l : lockedBy) {
			if (l.tid.equals(tid))
				return true;
		}
		return false;
	}
	

	/**
	 * detects deadlock
	 * 
	 * @param tid
	 * @param pid
	 * @return
	 */
	private synchronized boolean detectDeadlock(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		
		// no transaction occupying the lock on pid
		if (lockedBy == null || lockedBy.size() == 0)
			return false;
		
		// check each transaction t is waiting for
		for (Locks l : lockedBy) {
			PageId p = dpGraph.get(l.tid);
			if (detectCycle(l.tid, p)) {
				return true;
			}
		}
		
		return false;

	}
	
	static final int WHITE = 0;
	static final int GRAY = 1;
	static final int BLACK = 2;
	
	
	
	
	
	private synchronized boolean detectCycle(TransactionId tid, PageId pid) {
		ArrayList<Locks> lockedBy = locking.get(pid);
		
		if (lockedBy == null || lockedBy.size() == 0)
			return false;
		
		for (Locks l : lockedBy) {
			
		}
	}

}
