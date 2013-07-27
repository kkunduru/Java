package com.cloudwick.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomKeyComparator extends WritableComparator {

	public CustomKeyComparator() {
		super(TextId.class,true);

	}

	public int compare(WritableComparable keyA, WritableComparable keyB) {

		TextId obj1 = (TextId) keyA;
		TextId obj2 = (TextId) keyB;

		return obj1.compareTo(obj2);
	}

}
