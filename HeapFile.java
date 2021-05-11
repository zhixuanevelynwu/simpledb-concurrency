package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f =f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if(pid.getTableId() != getId()){
            throw new IllegalArgumentException("Page not found in table!");
        }
        int pageNum = pid.getPageNumber();
        if(pageNum<0 || pageNum> numPages()){
            throw new IllegalArgumentException("Page number is out of range!");
        }

        byte []pageData = HeapPage.createEmptyPageData();

        try{
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            try{
                raf.seek(BufferPool.getPageSize() * pageNum);
                raf.read(pageData,0,BufferPool.getPageSize());
                return new HeapPage(new HeapPageId(getId(),pageNum),pageData);
            }finally {
                raf.close();
            }
            }catch (IOException e) {
                throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] pdata = page.getPageData();
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        try{
            RandomAccessFile raf = new RandomAccessFile(f,"rw");
            raf.seek(offset);
            raf.write(pdata, 0, BufferPool.getPageSize());
            raf.close();          
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	//System.out.println((int) (this.f.length() / BufferPool.getPageSize()));
        return (int) (this.f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        ArrayList<Page> emptyPages = new ArrayList<Page>();
        ArrayList<Page> updatedPages = new ArrayList<Page>();
        try{
            for (int i = 0; i < this.numPages(); i++){
                HeapPageId pid = new HeapPageId(this.getId(),i);
                HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                if (hp.getNumEmptySlots() > 0){
                    emptyPages.add(hp);
                }
            }
            if (emptyPages.isEmpty()){
                HeapPageId hpid = new HeapPageId(this.getId(),this.numPages());
                HeapPage hp = new HeapPage(hpid, HeapPage.createEmptyPageData());
                hp.insertTuple(t);
                this.writePage(hp);
                updatedPages.add(hp);
            }
            else{
                HeapPage hp = (HeapPage) emptyPages.get(0);
                hp.insertTuple(t);
                updatedPages.add(hp);
            }
        }
        catch (DbException e){
            e.printStackTrace();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (TransactionAbortedException e){
            e.printStackTrace();
        }
        
        return updatedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // return null;
        // not necessary for lab1
        ArrayList<Page> updatedPages = new ArrayList<Page>();
        PageId pid = t.getRecordId().getPageId();
        try{
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            hp.deleteTuple(t);
            updatedPages.add(hp);
        }
        catch (DbException e){
            e.printStackTrace();
        }
        catch (TransactionAbortedException e){
            e.printStackTrace();
        } 
        return updatedPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pageNum = -1;
            private Iterator<Tuple> tupleIterator = null;
            private final BufferPool pool = Database.getBufferPool();
            private final int tableId  = getId();

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNum = 0;
                tupleIterator = ((HeapPage)pool.getPage(tid,new HeapPageId(tableId,pageNum++),Permissions.READ_ONLY)).iterator();

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(tupleIterator != null && tupleIterator.hasNext()){
                    return true;
                }
                else if(pageNum<0 || pageNum >numPages()){
                    return false;
                }
                else{
                    tupleIterator = ((HeapPage)pool.getPage(tid,new HeapPageId(tableId,pageNum++),Permissions.READ_ONLY)).iterator();
                    return hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (hasNext()){
                    return tupleIterator.next();
                }
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pageNum = -1;
                tupleIterator = null;
            }
        };
    }

}