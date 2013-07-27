package com.cloudwick.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupComparator extends WritableComparator {

	protected CustomGroupComparator() {
		super(TextId.class, true);
	}
	
	public int compare(WritableComparable a, WritableComparable b){
		
		TextId compositeKeyA = (TextId) a;
		TextId compositeKeyB = (TextId) b;
		
		return compositeKeyA.compareTo(compositeKeyB,"compare on deptId");
		
	}

}
