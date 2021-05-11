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

	public void removeLocksBy(TransactionId tid) {
		for (Map.Entry<PageId, ArrayList<Locks>> e : locking.entrySet()) {
			ArrayList<Locks> locks = e.getValue();
			for (Locks l : locks) {
				if (l.tid.equals(tid)) {
					unlock(tid, e.getKey());
					break;
				}
			}
		}
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
		// System.out.println("Try to lock on Tramsaction#" + tid + " page " + pid);
		// dpGraph.put(tid, pid);
		boolean locked = (perm == S) ? SLock(tid, pid) : XLock(tid, pid);
		// dpGraph.put(tid, pid);
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
				System.out.println(tid + " released page " + pid);
				lockedBy.remove(l);
				locking.put(pid, lockedBy);
				return true;
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

		System.out.println();
		System.out.println("dpGraph: " + dpGraph);
		System.out.println(tid + " waiting for: " + lockedBy + " caused by: " + cause);
		System.out.println();

		// if (cause != null && cause.contains(tid)) {
		if (cause != null && cause.contains(tid)) {
			dpGraph.remove(tid);
			//removeLocksBy(tid);
			return true;
		}

		return false;
	}

	final int WHITE = 0;
	final int GRAY = 1;
	final int BLACK = 2;
	int t;

	synchronized void recDFS(TransactionId tid) {
		NodeInfo info = graphInfo.get(tid);

		if (info == null) {
			info = new NodeInfo(tid);
			graphInfo.put(tid, info);
		}
		info.color = GRAY;
		info.d = ++t;

		PageId page = dpGraph.get(tid);
		if (page == null)
			return; // !!!

		ArrayList<Locks> lockedBy = locking.get(page);
		if (lockedBy == null)
			return; // !!!

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

	synchronized void DFS() {
		for (TransactionId s : dpGraph.keySet()) {
			if (graphInfo.get(s) == null || graphInfo.get(s).color == WHITE) {
				recDFS(s);
			}
		}
	}

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
				// System.out.println("CYCLE: " + cycle);
				return cycle;
			}
		}
		graphInfo.clear();
		return null;
	}
	
	//----------------TEST CODE------------------
	public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid) {//T1为tid，P3为pid
        List<Locks> holders = locking.get(pid);
        if (holders == null || holders.size() == 0) {
            return false;
        }
        List<PageId> pids = getAllLocksByTid(tid);//找出T1拥有的所有资源，即只含有P1的list
        for (Locks ls : holders) {
            TransactionId holder = ls.tid;
            //去掉T1，因为虽然上图没画出这种情况，但T1可能同时也在其他Page上有读锁，这会影响判断结果
            if (!holder.equals(tid)) {
                //判断T3(holder)是否直接或间接在等待P1(pids)
                //由图可以看出T3在直接等待P2，而P2的拥有者T2在直接等待P1,即T3在间接等待P1
                boolean isWaiting = isWaitingResources(holder, pids, tid);
                if (isWaiting) {
                    return true;
                }
            }
        }
        return false;
    }
	
	private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (Map.Entry<PageId, ArrayList<Locks>> entry : locking.entrySet()) {
            for (Locks ls : entry.getValue()) {
                if (ls.tid.equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
		return pids;
	}

	private synchronized boolean isWaitingResources(TransactionId tid, List<PageId> pids, TransactionId toRemove) {
		PageId waitingPage = dpGraph.get(tid);
		if (waitingPage == null) {
			return false;
		}
		for (PageId pid : pids) {
			if (pid.equals(waitingPage)) {
				return true;
			}
		}
		// 到达这里说明tid并不直接在等待pids中的任意一个，但有可能间接在等待
		// 如果waitingPage的拥有者们(去掉toRemove)中的某一个正在等待pids中的某一个，说明是tid间接在等待
		List<Locks> holders = locking.get(waitingPage);
		if (holders == null || holders.size() == 0)
			return false;// 该资源没有拥有者
		for (Locks ls : holders) {
			TransactionId holder = ls.tid;
			if (!holder.equals(toRemove)) {// 去掉toRemove，在toRemove刚好拥有waitingResource的读锁时就需要
				boolean isWaiting = isWaitingResources(holder, pids, toRemove);
				if (isWaiting)
					return true;
			}
		}
		// 如果在for循环中没有return，说明每一个holder都不直接或间接等待pids
		// 故tid也非间接等待pids
		return false;
	}


}
