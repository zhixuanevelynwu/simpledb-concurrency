package simpledb;

import java.io.IOException;

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
    private boolean accessed;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.child = child;
    	this.tid = t;
    	this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
    	this.deleted = false;
    	this.count = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    	this.accessed = false;
    	while (child.hasNext()) {
            Tuple next = child.next();
            try {
				Database.getBufferPool().deleteTuple(tid, next);
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            count++;
        }
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.accessed = false;
    	child.rewind();
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
        if (accessed) {
            return null;
        }
        accessed = true;
        Tuple deletedTuples=new Tuple(getTupleDesc());
        deletedTuples.setField(0,new IntField(count));
        return deletedTuples;
    	/*if (deleted) {
        	return null;
        } else {
        	
        	Tuple tuple = new Tuple(td);
        	int count = 0;
        	//BufferPool buf = Database.getBufferPool();
        	
        	while (child.hasNext()) {
        		try {
        			Database.getBufferPool().deleteTuple(tid, child.next());
				} catch (IOException e) {
					throw new DbException("delete tuple failed");
				}
        		++count;
        	}
        	
        	tuple.setField(0, new IntField(count));
        	deleted = true;
        	return tuple;
        }*/
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