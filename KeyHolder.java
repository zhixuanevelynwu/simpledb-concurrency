package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Keyholder manages which transactions gets access to a S or X lock
 */
public class KeyHolder {
	Permissions S = Permissions.READ_ONLY;
	Permissions X = Permissions.READ_WRITE;

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

		public String toString() {
			String permission;
			if (this.perm == Permissions.READ_ONLY)
				permission = "S";
			else
				permission = "X";
			return "tid=" + this.tid.toString() + " permission=" + permission;
		}
	}

	// stores information the transactions that are accessing each page
	// -> page pid is locked by which locks
	public ConcurrentHashMap<PageId, ArrayList<Locks>> locking;
	// a waiting table
	public ConcurrentHashMap<TransactionId, PageId> dpGraph;
	// graph information
	private ConcurrentHashMap<TransactionId, NodeInfo> graphInfo;

	public KeyHolder() {
		this.locking = new ConcurrentHashMap<>();
		this.dpGraph = new ConcurrentHashMap<>();
		this.graphInfo = new ConcurrentHashMap<>();
	}

	/**
	 * lock a transaction
	 * 
	 * @param tid
	 * @param pid
	 * @param perm
	 * @return
	 * @throws InterruptedException
	 * @throws TransactionAbortedException
	 */
	public synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm)
			throws InterruptedException, TransactionAbortedException {
		boolean locked = (perm == S) ? SLock(tid, pid) : XLock(tid, pid);
		while (!locked) {
			// deadlock check
			if (handleDeadlock(tid, pid)) {
				throw new TransactionAbortedException();
			}
			// wait for a while
			Thread.sleep(500);
			// try to lock again
			locked = (perm == S) ? SLock(tid, pid) : XLock(tid, pid);
		}
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
				//System.out.println(tid + " released page " + pid);
				lockedBy.remove(l);
				locking.put(pid, lockedBy);
				return true;
			}
		}
		return false;
	}
	
	/**
	 * release all locks by a transaction
	 * @param tid
	 */
	public synchronized void releaseAllLocksBy(TransactionId tid) {
		ArrayList<PageId> pids = new ArrayList<>();
		for (Map.Entry<PageId, ArrayList<Locks>> entry : locking.entrySet()) {
			for (Locks ls : entry.getValue()) {
				if (ls.tid.equals(tid)) {
					pids.add(entry.getKey());
				}
			}
		}

		for (PageId pid : pids) {
			unlock(tid, pid);
		}
	}

	/**
	 * grant S lock on page pid to transaction tid
	 * 
	 * @param tid
	 * @param pid
	 * @return true if granted
	 */
	public synchronized boolean SLock(TransactionId tid, PageId pid) {
		// System.out.println("SLock on " + tid + " page " + pid);
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
		// System.out.println("XLock on " + tid + " page " + pid);
		ArrayList<Locks> lockedBy = locking.get(pid);
		if (lockedBy != null && lockedBy.size() > 0) {
			// only one or two locks -> mine or not mine
			if (lockedBy.size() == 1) {
				Locks l = lockedBy.get(0);
				// is it my lock?
				if (l.tid.equals(tid)) {
					return (l.perm == X) ? true : lockHelper(pid, tid, X);
				} else {
					// !!!
					dpGraph.put(tid, pid);
					return false;
				}
			} else if (lockedBy.size() == 2) {
				for (Locks l : lockedBy) {
					if (l.tid.equals(tid) && l.perm == X) {
						return true;
					}
				}
				dpGraph.put(tid, pid);
				return false;
			}
			// beyond 2 locks -> impossible to grant permission
			dpGraph.put(tid, pid);
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
		if (lockedBy == null)
			lockedBy = new ArrayList<>();
		lockedBy.add(mylock);
		locking.put(pid, lockedBy);
		dpGraph.remove(tid);
		// System.out.println("Grant Transaction #" + tid + " lock on " + pid);
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
	 * @throws TransactionAbortedException
	 */
	public synchronized boolean handleDeadlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
		ArrayList<Locks> lockedBy = locking.get(pid);

		// no transaction occupying the lock on pid
		if (lockedBy == null || lockedBy.size() == 0)
			return false;

		// check each transaction t is waiting for
		HashSet<TransactionId> cause = detectCycle();

		//System.out.println(tid + " waiting for: " + lockedBy + " caused by: " + cause);

		if (cause != null && cause.contains(tid)) {
			dpGraph.remove(tid);
			return true;
		}

		return false;
	}
	
	/**
	 * store each transaction as nodes in a dpgraph
	 * helps to identify cycles(deadlocks) among the waiting transactions
	 * 
	 * @author xx
	 *
	 */
	private static class NodeInfo {
		int color;
		TransactionId tid;
		TransactionId parent;
		int d;
		int f;

		NodeInfo(TransactionId tid) {
			this.color = 0;
			this.tid = tid;
		}

		public String toString() {
			return "tid=" + tid + " parent=" + parent + " color=" + color + " d=" + d + " f=" + f;
		}
	}

	final int WHITE = 0;
	final int GRAY = 1;
	final int BLACK = 2;
	int t;
	
	/**
	 * performs DFS on dpgraph
	 * @param tid
	 */
	synchronized void recDFS(TransactionId tid) {
		NodeInfo info = graphInfo.get(tid);

		if (info == null) {
			info = new NodeInfo(tid);
			graphInfo.put(tid, info);
		}
		info.color = GRAY;
		info.d = ++t;

		PageId page = dpGraph.get(tid);
		if (page == null) return; // !!!

		ArrayList<Locks> lockedBy = locking.get(page);
		if (lockedBy == null) return; // !!!

		for (Locks l : lockedBy) {
			TransactionId v = l.tid;
			NodeInfo vInfo = graphInfo.get(v);

			if (vInfo == null) {
				vInfo = new NodeInfo(tid);
				graphInfo.put(tid, vInfo);
			}

			if (vInfo.color == WHITE) {
				vInfo.parent = tid;
				graphInfo.put(v, vInfo);
				recDFS(v);
			}
		}

		info.color = BLACK;
		info.f = ++t;
		graphInfo.put(tid, info);
	}
	
	/**
	 * performs full DFS on dpgraph
	 */
	synchronized void DFS() {
		for (TransactionId s : dpGraph.keySet()) {
			if (graphInfo.get(s) == null || graphInfo.get(s).color == WHITE) {
				recDFS(s);
			}
		}
	}

	/**
	 * identify the cause of the cycle
	 * in the dpgraph, these are the transactions that cause the deadlock
	 * @param tid
	 * @return
	 */
	private synchronized boolean cycle(TransactionId tid) {
		NodeInfo info = graphInfo.get(tid);
		PageId page = dpGraph.get(tid);
		ArrayList<Locks> lockedBy = locking.get(page);

		if (lockedBy == null)
			return false;

		for (Locks v : lockedBy) {
			NodeInfo vInfo = graphInfo.get(v.tid);

			if (vInfo.d <= info.f && info.d < info.f && vInfo.d <= info.d && info.f <= vInfo.f) {
				return true;
			}
		}

		return false;
	}
	
	/**
	 * constructs a cycle out of all the relevant transactions
	 * @return
	 */
	private synchronized HashSet<TransactionId> detectCycle() {
		DFS();
		for (TransactionId u : dpGraph.keySet()) {
			if (cycle(u)) {
				HashSet<TransactionId> cycle = new HashSet<>();
				TransactionId current = u;
				do {
					cycle.add(current);
					current = graphInfo.get(current).parent;
				} while (current != null && !current.equals(u));
				graphInfo.clear();
				return cycle;
			}
		}
		graphInfo.clear();
		return null;
	}

}
