package com.cloudwick.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SSEmpMapper extends Mapper<LongWritable, Text, TextId, Text> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(",");
		String str = arr[0] + "," + arr[1];
		context.write(new TextId(new Text(arr[4]),new IntWritable(1)), new Text(str));
	}

}
