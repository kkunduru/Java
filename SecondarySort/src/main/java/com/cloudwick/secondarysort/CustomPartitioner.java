package com.cloudwick.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<TextId, Text> {

	@Override
	public int getPartition(TextId key, Text record, int numberOfPartitions) {
		
		
		return (key.getDeptId().hashCode()%numberOfPartitions);
		
				
	}
	
	

}
