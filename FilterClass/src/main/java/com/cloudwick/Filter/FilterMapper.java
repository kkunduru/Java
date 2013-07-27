package com.cloudwick.Filter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] arr = str.split(",");
		int salary = Integer.parseInt(arr[3]);
		if (salary > 3000) {
			context.write(null, new Text(str));
		}
	}

}
