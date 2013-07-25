package com.cloudwick.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SSReducer extends Reducer<TextId, Text, NullWritable, Text> {

	protected void reduce(TextId key, Iterable<Text> rows, Context context)
			throws IOException, InterruptedException {
		String deptName = null;
		boolean set = false;

		for (Text row : rows) {
			if (!set) {
				deptName = row.toString().split(",")[0];
				set = true;
			} else {
				String[] record = row.toString().split(",");
				context.write(null, new Text(record[0] + "," + record[1] + ","
						+ deptName));
			}
	}
}
}