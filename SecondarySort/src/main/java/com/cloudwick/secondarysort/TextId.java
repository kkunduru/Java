package com.cloudwick.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextId implements WritableComparable<TextId> {

	private Text deptId;
	private IntWritable identifier;

	public TextId() {
		set(new Text(), new IntWritable());
		System.out.println("Reached the text id constructor");
	}

	public void set(Text deptId, IntWritable identifier) {
		this.deptId = deptId;
		this.identifier = identifier;
	}

	public TextId(Text deptId, IntWritable identifier) {
		System.out.println("Reached the text id constructor");
		this.deptId = deptId;
		this.identifier = identifier;
	}

	public Text getDeptId() {
		return deptId;
	}

	public IntWritable getIdentifier() {
		return identifier;
	}

	public void readFields(DataInput arg0) throws IOException {
		deptId.readFields(arg0);
		identifier.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		deptId.write(arg0);
		identifier.write(arg0);
	}

	public int hashCode() {
		return deptId.hashCode() * 163 + identifier.hashCode();
	}

	public boolean equals(Object o) {
		if (o instanceof TextId) {
			TextId ob1 = (TextId) o;
			return deptId.equals(ob1.deptId);
		}
		return false;
	}

	public int compareTo(TextId obj) {

		int result = this.deptId.compareTo(obj.deptId);

		if (result != 0) {
			return result;
		} else {
			return this.identifier.compareTo(obj.identifier);
		}

	}

	public int compareTo(TextId obj, String x) {
		if (x.equals("compare on deptId")) {

			return this.deptId.compareTo(obj.deptId);

		} else {
			return this.identifier.compareTo(obj.identifier);
		}
	}
}
