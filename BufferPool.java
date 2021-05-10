package simpledb;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
import java.util.Map.Entry;

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
	//private PageId recentPid;

	Permissions S = Permissions.READ_ONLY;
	Permissions X = Permissions.READ_WRITE;

	/**
	 * a pool of pages
	 */
	private final ConcurrentHashMap<PageId, Page> pages = new ConcurrentHashMap<>();;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		this.numPages = numPages;
		this.keyHolder = new KeyHolder();
		
		lockManager = new LockManager();
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

	private final KeyHolder keyHolder;

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire a
	 * lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is present,
	 * it should be returned. If it is not present, it should be added to the buffer
	 * pool and returned. If there is insufficient space in the buffer pool, a page
	 * should be evicted and the new page should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 * @throws InterruptedException
	 */
	
	//锁管理器
    private final LockManager lockManager;
	public Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		// some code goes here
		// block and acquire desired lock before returning a page
		/*if (perm == S) System.out.println(tid + " tries to Slock on page " + pid);
		if (perm == X) System.out.println(tid + " tries to Xlock on page " + pid);
		 
		try {
			keyHolder.dpGraph.put(tid, pid);
			keyHolder.lock(tid, pid, perm);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (perm == S) System.out.println(tid + " Slocks on page " + pid);
		if (perm == X) System.out.println(tid + " Xlocks on page " + pid);
		*/
		//edit
		// some code goes here
        boolean result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                : lockManager.grantXLock(tid, pid);
        //下面的while循环就是在模拟等待过程，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
        while (!result) {
            if (lockManager.deadlockOccurred(tid, pid)) {
            	System.out.println();
            	System.out.println("DEADLOCK OCCURED");
            	System.out.println();
                throw new TransactionAbortedException();
            }
            try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            //sleep之后再次判断result
            result = (perm == Permissions.READ_ONLY) ? lockManager.grantSLock(tid, pid)
                    : lockManager.grantXLock(tid, pid);
        }
		//done edit
		
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
		//recentPid = pid;
		return p;
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result in
	 * wrong behavior. Think hard about who needs to call this and why, and why they
	 * can run the risk of calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		keyHolder.unlock(tid, pid);
	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		return keyHolder.exists(tid, p);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		if (commit) { // commit -> flush dirty pages to disk and release
			for (Map.Entry<PageId, Page> e : pages.entrySet()) {
				Page p = e.getValue();
				PageId pid = e.getKey();
				if (holdsLock(tid, pid)) releasePage(tid, pid);
				if (p.isDirty() != null && p.isDirty().equals(tid)) {
					flushPage(pid);
				}
			}
		} else { // abort -> reread page from disk and release
			for (Map.Entry<PageId, Page> e : pages.entrySet()) {
				Page p = e.getValue();
				PageId pid = e.getKey();
				if (holdsLock(tid, pid)) releasePage(tid, pid);
				if (p.isDirty() != null && p.isDirty().equals(tid)) {
					DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId()); 
					e.setValue(f.readPage(pid));
				}
			}
		}
	}

	/**
	 * Add a tuple to the specified table on behalf of transaction tid. Will acquire
	 * a write lock on the page the tuple is added to and any other pages that are
	 * updated (Lock acquisition is not needed for lab2). May block if the lock(s)
	 * cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		try {
			HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
			ArrayList<Page> updatedPages = file.insertTuple(tid, t);
			for (Page p : updatedPages) {
				p.markDirty(true, tid);
				pages.put(p.getId(), p);
			}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write lock on
	 * the page the tuple is removed from and any other pages that are updated. May
	 * block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling their
	 * markDirty bit, and adds versions of any pages that have been dirtied to the
	 * cache (replacing any existing versions of those pages) so that future
	 * requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		try {
			HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
			ArrayList<Page> updatedPages = file.deleteTuple(tid, t);
			for (Page p : updatedPages) {
				p.markDirty(true, tid);
				pages.put(p.getId(), p);
			}
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
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
	 * 
	 * Also used by B+ tree files to ensure that deleted pages are removed from the
	 * cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		if (this.pages.containsKey(pid)) {
			this.pages.remove(pid);
		}
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
		for (PageId pid : pages.keySet()) {
			if (pages.get(pid).isDirty() == null) {
				this.discardPage(pid);
				break;
			}
		}
	}

}