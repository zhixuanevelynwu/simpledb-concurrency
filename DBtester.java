package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.systemtest.SystemTestUtil;

public class DBtester {
	public static void main(String[] args) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
		
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
		map.put("ant", 1);
		map.put("bee", 2);
		map.put("ant", -10);
		System.out.println(map);
		  
        

	}
	    public static List<Integer> merge(List<Integer> nums1, List<Integer> nums2) {
	    	int l1 = nums1.size();
	        int l2 = nums2.size();
	        int i=0, j=0;
	        List<Integer> list = new ArrayList<>();
	        while (i < l1 && j < l2) {
	            int x = nums1.get(i);
	            int y = nums2.get(j);
	            if (x < y) {
	                list.add(x);
	                i++;
	            } else {
	                list.add(y);
	                j++;
	            }
	        }
	        if (j < l2) {
	            for (int n = j; n < l2; n++) {
	                list.add(nums2.get(n));
	            }
	        } 
	        if (i < l1) {
	            for (int n = i; n < l1; n++) {
	                list.add(nums1.get(n));
	            }
	        }
	        return list;
	    }
}
