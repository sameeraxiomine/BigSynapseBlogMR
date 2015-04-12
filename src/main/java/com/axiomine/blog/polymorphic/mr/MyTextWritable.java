package com.axiomine.blog.polymorphic.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public abstract class MyTextWritable implements Writable {
    public String str="";
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChars(str);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	str = in.readLine();
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((str == null) ? 0 : str.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	MySingleTextWritable other = (MySingleTextWritable) obj;
	if (str == null) {
	    if (other.str != null)
		return false;
	} else if (!str.equals(other.str))
	    return false;
	return true;
    }
}