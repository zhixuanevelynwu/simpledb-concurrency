package simpledb;

//import required packages
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Object group;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if(gbfield == Aggregator.NO_GROUPING) {
    		this.group = new ArrayList<Integer>();
    	}
    	else {
    		this.group = new HashMap<Field,ArrayList<Integer>>();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    
    @SuppressWarnings("unchecked")
	public void mergeTupleIntoGroup(Tuple tup) {
        // works with the tuple schema and merges them using an Array list and HashMap depending on the GROUP_BY clause
    	if(gbfield == Aggregator.NO_GROUPING) {
    		((ArrayList<Integer>)group).add(((IntField)tup.getField(afield)).getValue());
    	}
    	else {
    		HashMap<Field,ArrayList<Integer>> groupMap = (HashMap<Field,ArrayList<Integer>>)group;
    		Integer aggValue = ((IntField)tup.getField(afield)).getValue();
    		groupMap.computeIfAbsent(tup.getField(gbfield), k -> new ArrayList<Integer>()).add(aggValue);
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    	class iterator implements OpIterator {
    		
    		/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			private ArrayList<Tuple> list = new ArrayList<Tuple>();
    		private Iterator<Tuple> iterator = null;
    		
    		//function to feed the actual values produced by getAggValue into the resp. data structures
    		@SuppressWarnings("unchecked")
			public iterator(){
    			if(gbfield == Aggregator.NO_GROUPING) {
    				Tuple tuple = new Tuple(getTupleDesc());
    				tuple.setField(0, new IntField(getAggValue((ArrayList<Integer>)group)));
    				list.add(tuple);
    			}
    			else {
    				HashMap<Field,ArrayList<Integer>> groupMap = (HashMap<Field,ArrayList<Integer>>)group;
    				for(Map.Entry<Field, ArrayList<Integer>> entry : groupMap.entrySet()) {
    					Tuple td = new Tuple(getTupleDesc());
    					
    					Field groupField = entry.getKey();
    					td.setField(0, groupField);
    					Field afield = new IntField(getAggValue(entry.getValue()));
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
    		
    		//switch statement for each operator
    		private int getAggValue(List<Integer> l) {
    			int elem = 0;
    			switch(what) {
    			case COUNT:
    				elem = l.size();
    				break;
    			case SUM:
    				for(int i:l) {
    					elem+=i;
    				}
    				break;
    			case AVG:
    				for(int i:l) {
    					elem+=i;
    				}
    				elem = elem/l.size();
    				break;
    			case MIN:
    				elem = l.get(0);
    				for(int i :l) {
    					
    					if(elem>i) elem = i;
    				}
   					break;
   				case MAX:
   					elem = l.get(0);
   					for(int i:l) {
    					if(elem<i) elem = i;
    				}
    				break;
    			default:
    				break;
    			}
    			return elem;
    		}
    	}
    	
    	return new iterator();
    }

}