package com.cloudwick.groupon;

import java.io.IOException;
import java.util.HashSet;
//import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class URLReducer extends Reducer<Text, Text, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		HashSet<String> hs = new HashSet<String>();
		for (Text value : values) {
			hs.add(value.toString());
					
		}
		int size = hs.size();
		context.write(key, new IntWritable(size));
	}

}
