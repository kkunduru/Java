package com.cloudwick.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SSDeptMapper extends Mapper<LongWritable, Text, TextId, Text> {
	
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(",");
			
		context.write(new TextId(new Text(arr[0]),new IntWritable(0)), new Text(arr[1]));
	}

}
