package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private final TupleDesc td;
    private boolean deleted;
    private int count;

	/**
	 * Constructor specifying the transaction that this delete belongs to as well as
	 * the child to read from.
	 * 
	 * @param t     The transaction this delete runs in
	 * @param child The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, OpIterator child) {
		// some code goes here
		this.tid = t;
		this.child = child;
		this.count = 0;
		this.td = new TupleDesc(new Type[] { Type.INT_TYPE });
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
		
		deleted = false;
		BufferPool buf = Database.getBufferPool();
		while (child.hasNext()) {
			Tuple next = child.next();
			buf.deleteTuple(tid, next);
			count ++;
		}
	}

	public void close() {
		// some code goes here
		super.close();
		child.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		deleted = false;
	}

	/**
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (deleted) {
			return null;
		}
		deleted = true;
		Tuple next = new Tuple(getTupleDesc());
		next.setField(0, new IntField(count));
		return next;
	}

	@Override
	public OpIterator[] getChildren() {
		// some code goes here
        return new OpIterator[] { child };
	}

	@Override
	public void setChildren(OpIterator[] children) {
		// some code goes here
    	if (children == null)
			throw new IllegalArgumentException("Argument should contain 1 children");
    	
    	child = children[0];
    	deleted = false;
	}

}