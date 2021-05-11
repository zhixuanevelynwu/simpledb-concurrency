package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private Object group;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    @SuppressWarnings("deprecation")
	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	
    	if(what!=Aggregator.Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if(gbfield == Aggregator.NO_GROUPING) {
    		this.group = new Integer(0);
    	}
    	else {
    		this.group = new HashMap<Field,Integer>();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("unchecked")
	public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(gbfield == Aggregator.NO_GROUPING) {
    		group = (Integer)group + 1;
    	}
    	else {
    		HashMap<Field,Integer> groupMap = (HashMap<Field,Integer>)group;
    		Field f = tup.getField(gbfield);
    		if(groupMap.containsKey(f)) {
    			groupMap.put(f, groupMap.get(f)+1);
    		}
    		else {
    			groupMap.put(f,1);
    		}
    		
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
class iterator implements OpIterator {
    		
    		/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			private ArrayList<Tuple> list = new ArrayList<Tuple>();
    		private Iterator<Tuple> iterator = null;
    		
    		@SuppressWarnings("unchecked")
			public iterator(){
    			if(gbfield == Aggregator.NO_GROUPING) {
    				Tuple tuple = new Tuple(getTupleDesc());
    				tuple.setField(0, new IntField((Integer)group));
    				list.add(tuple);
    			}
    			else {
    				HashMap<Field,Integer> groupMap = (HashMap<Field,Integer>)group;
    				for(Map.Entry<Field, Integer> entry : groupMap.entrySet()) {
    					Tuple td = new Tuple(getTupleDesc());
    					
    				Field groupField = entry.getKey();
    				td.setField(0, groupField);
    				Field afield = new IntField(entry.getValue());
    				td.setField(1, afield);
    				list.add(td);
    				}
    			}
    		}
    		
    		@Override
    		public void open() throws DbException, TransactionAbortedException{
    			iterator  = list.iterator();
    		}
    		
    		@Override
    		public boolean hasNext() throws DbException, TransactionAbortedException{
    			if(iterator == null) {
    				throw new IllegalStateException();
    			}
    			return iterator.hasNext();
    		}
    		
    		@Override
    		public Tuple next() throws DbException, TransactionAbortedException{
    			if(iterator == null) {
    				throw new IllegalStateException();
    			}
    			return iterator.next();
    		}
    		
    		@Override
    		public void rewind() throws DbException, TransactionAbortedException{
    			iterator  = list.iterator();
    		}
    		
    		@Override
    		public TupleDesc getTupleDesc() {
    			if(gbfield == Aggregator.NO_GROUPING) {
    				return new TupleDesc(new Type[] {Type.INT_TYPE});
    			}
    			else {
    				return new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    			}
    		}
    		
    		@Override
    		public void close() {
    			iterator = null;
    		}
    		
    	}
    	
    	return new iterator();
    }

}