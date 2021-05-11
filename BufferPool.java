package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

	private static int pageSize = DEFAULT_PAGE_SIZE;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	/**
	 * the number of pages stored in the pool
	 */
	private final int numPages;
	/**
	 * "the bufferpool"
	 */
	private final ConcurrentHashMap<PageId, Page> pages;
	/**
	 * lru eviction policy
	 */
	private final ConcurrentHashMap<PageId, Integer> recentlyUsed;
	/**
	 * in charge of which transactions get to lock
	 */
	private final KeyHolder keyHolder;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		this.pages = new ConcurrentHashMap<>();
		this.numPages = numPages;
		this.keyHolder = new KeyHolder();
		this.recentlyUsed = new ConcurrentHashMap<>();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned. If there is insufficient space in the buffer pool, an page
	 * should be evicted and the new page should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 * @see LockManager#grantSLock(TransactionId, PageId)
	 * @see LockManager#grantXLock(TransactionId, PageId)
	 */
	Permissions S = Permissions.READ_ONLY;
	Permissions X = Permissions.READ_WRITE;

	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		boolean locked = (perm == S) ? keyHolder.SLock(tid, pid) : keyHolder.XLock(tid, pid);
		while (!locked) {
			// wait for a while
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
			// try to lock again
			locked = (perm == S) ? keyHolder.SLock(tid, pid) : keyHolder.XLock(tid, pid);
			// deadlock check
			if (keyHolder.handleDeadlock(tid, pid)) {
				throw new TransactionAbortedException();
			}
		}
		Page p = pages.get(pid);
		// not in the buffer
		if (p == null) {
			if (pages.size() == numPages) {
				this.evictPage();
			}
			// find the page
			Catalog catalog = Database.getCatalog();
			int tableID = pid.getTableId();
			p = catalog.getDatabaseFile(tableID).readPage(pid);
			pages.put(pid, p);
		}
		recentlyUsed.put(pid, 0);
		return p;

	}

	public static int getPageSize() {
		return pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}

	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = DEFAULT_PAGE_SIZE;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for proj1
		keyHolder.unlock(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public synchronized void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for proj1
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for proj1
		// return lockManager.getLockState(tid, p) != null;
		return keyHolder.exists(tid, p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		keyHolder.releaseTransactionLocks(tid);
		if (commit) {
			System.out.println("Transaction " + tid + " COMPLETED");
			for (Map.Entry<PageId, Page> e : pages.entrySet()) {
				Page p = e.getValue();
				PageId pid = e.getKey();
				if (holdsLock(tid, pid))
					releasePage(tid, pid);
				if (p.isDirty() != null && p.isDirty().equals(tid)) {
					flushPage(pid);
				}
			}
		} else {
			for (Entry<PageId, Page> e : pages.entrySet()) {
				Page p = e.getValue();
				PageId pid = e.getKey();
				if (holdsLock(tid, pid))
					releasePage(tid, pid);
				if (p.isDirty() != null && p.isDirty().equals(tid)) {
					DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
					e.setValue(f.readPage(pid));
				}
			}
		}
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will acquire a
	 * write lock on the page the tuple is added to(Lock acquisition is not needed
	 * for lab2). May block if the lock cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and updates cached versions of any pages that have been
	 * dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> affectedPages = table.insertTuple(tid, t);
		for (Page page : affectedPages) {
			page.markDirty(true, tid);
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from. May block if the lock cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit. Does not need to update cached versions of any pages that have
	 * been dirtied, as it is not possible that a new page was created during the
	 * deletion (note difference from addTuple).
	 *
	 * @param tid the transaction adding the tuple.
	 * @param t   the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for proj1
		int tableId = t.getRecordId().getPageId().getTableId();
		HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
		Page affectedPage = table.deleteTuple(tid, t);
		affectedPage.markDirty(true, tid);
	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes
	 * dirty data to disk so will break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		for (PageId pid : this.pages.keySet()) {
			Page page = this.pages.get(pid);
			if (page.isDirty() != null) {
				this.flushPage(pid);
			}
		}
	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in its
	 * cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		this.pages.remove(pid);
	}

	/**
	 * Flushes a certain page to disk
	 * 
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		try {
			HeapFile hf = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
			Page page = this.pages.get(pid);
			if (page != null) {
				hf.writePage(page);
				page.markDirty(false, null);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
	}

	/**
	 * never evict a dirty page.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		Page evictedPage;
		int counter = -1;
		PageId evictedPageId = null;
		boolean isPageDirty = true;
		int dirtyPageCount = 0;

		for (PageId key : pages.keySet()) {
			if (pages.get(key).isDirty() != null) {
				dirtyPageCount++;
			}
		}
		if (dirtyPageCount == numPages) {
			throw new DbException("all pages in BufferPool are dirty.");
		}
		isPageDirty = true;
		// Check to make sure that the page evicted is not a dirty page
		for (PageId key : pages.keySet()) {
			int value = recentlyUsed.get(key);
			if (value > counter) {
				counter = value;
				evictedPageId = key;
				evictedPage = pages.get(evictedPageId);
				isPageDirty = ((HeapPage) evictedPage).isDirty() != null;
				if (!isPageDirty) {
					try {
						flushPage(evictedPageId);
						pages.remove(evictedPageId);
						recentlyUsed.remove(evictedPageId);
						break;

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

			}
		}

	}

}
