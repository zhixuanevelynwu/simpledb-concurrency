package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

	private static final long serialVersionUID = 1L;

	private TransactionId t;
	private OpIterator child;
	private int tableId;
	private TupleDesc td;

	/**
	 * Constructor.
	 *
	 * @param t       The transaction running the insert.
	 * @param child   The child operator from which to read tuples to be inserted.
	 * @param tableId The table in which to insert tuples.
	 * @throws DbException if TupleDesc of child differs from table into which we
	 *                     are to insert.
	 */
	public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
		// some code goes here
		if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
			throw new DbException("illegal TupleDesc");
		this.t = t;
		this.child = child;
		this.tableId = tableId;
		this.td = new TupleDesc(new Type[] { Type.INT_TYPE }, new String[] { "Inserted Records" });
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
		super.open();
	}

	public void close() {
		// some code goes here
		child.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
	}

	/**
	 * Inserts tuples read from child into the tableId specified by the constructor.
	 * It returns a one field tuple containing the number of inserted records.
	 * Inserts should be passed through BufferPool. An instances of BufferPool is
	 * available via Database.getBufferPool(). Note that insert DOES NOT need check
	 * to see if a particular tuple is a duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or null if
	 *         called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	/**
	 * checks if tuples have been inserted
	 */
	private boolean inserted = false;
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (inserted) return null;
		
		BufferPool buf = Database.getBufferPool();
		Tuple out = new Tuple(td);
		int count = 0;
		
		while (child.hasNext()) {
			try {
				buf.insertTuple(t, tableId, child.next());
			} catch (NoSuchElementException | DbException | IOException | TransactionAbortedException e) {
				e.printStackTrace();
			}
			count++;
		}
		inserted = true; // now that all are inserted, set to true
		out.setField(0, new IntField(count));
		return out;
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
	}
}
