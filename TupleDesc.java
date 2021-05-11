package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }
        
        /**
         * create anon TDItem
         * @param t
         */
        public TDItem(Type t) {
            this.fieldType = t;
			this.fieldName = null;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /**
     * A collection of TDItem.
     */
    private TDItem[] TDItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
		return Arrays.stream(TDItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	if (typeAr==null || typeAr.length < 1) throw new IllegalArgumentException("typeAr must contain at least one entry");
    	TDItem[] TDItems = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++) {
    		if (fieldAr == null || i >= fieldAr.length)
    			TDItems[i] = new TDItem(typeAr[i]);
    		else
    			TDItems[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    	this.TDItems = TDItems;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	if (typeAr==null || typeAr.length < 1) throw new IllegalArgumentException("typeAr must contain at least one entry");
    	int len = typeAr.length;
    	TDItem[] TDItems = new TDItem[len];
    	for (int i = 0; i < len; i++) {
    		TDItems[i] = new TDItem(typeAr[i]);
    	}
    	this.TDItems = TDItems;
    }

    /**
     * Constructor. Create a new tuple desc with TDItems.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    private TupleDesc(TDItem[] TDItems) {
        // some code goes here
    	this.TDItems = TDItems;
    }
    
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TDItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= TDItems.length) throw new NoSuchElementException("Invalid index");
        return this.TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= TDItems.length) throw new NoSuchElementException("Invalid index");
        return this.TDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	if (name == null) throw new NoSuchElementException("Field name cannot be null");
    	for (int i = 0; i < TDItems.length; i++) {
    		if (name.equals(TDItems[i].fieldName)) 
    			return i;
    	}
        throw new NoSuchElementException("No field with the matching name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;
    	for (int i = 0; i < TDItems.length; i ++) {
    		size += TDItems[i].fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	int len1 = td1.TDItems.length;
    	int len2 = td2.TDItems.length;
    	TDItem[] merge12 = new TDItem[len1+len2];
    	for (int i = 0; i < len1; i++) {
    		merge12[i] = td1.TDItems[i];
    	}
    	for (int j = len1; j < len1+len2; j++) {
    		merge12[j] = td2.TDItems[j-len1];
    	}
        return new TupleDesc(merge12);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	if (o==null) return false;
    	
    	try {
	    	TDItem[] ot = ((TupleDesc)o).TDItems;
	    	int len1 = TDItems.length;
	    	int len2 = ot.length;
	    	
	    	if (len1 != len2) return false;
	    	
	    	for (int i = 0; i < len1; i++) {
	    		if (!(TDItems[i].fieldType).equals(ot[i].fieldType))
	    			return false;
	    	}
	        return true;
        } catch (ClassCastException e) {
    		return false;
    	}
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        //throw new UnsupportedOperationException("unimplemented");
        return TDItems.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuilder sb = new StringBuilder();
    	int len = TDItems.length;
    	for (int i = 0; i < len-1; i++) {
    		sb.append(TDItems[i].fieldType + "(" + TDItems[i].fieldName + "), ");
    	}
    	sb.append(TDItems[len-1].fieldType + "(" + TDItems[len-1].fieldName + ")");
        return sb.toString();
    }
}
