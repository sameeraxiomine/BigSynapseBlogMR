package com.axiomine.blog.polymorphic.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyLongWritable extends MyNumberWritable {
    public Long lng = 0l;

    /*
     * Obligatary NoArgument Constructor so Reducer side and create Reduce side
     * key instance via reflection
     */
    public MyLongWritable() {

    }

    public MyLongWritable(long l) {
        lng = l;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(lng);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lng = in.readLong();
    }

    @Override
    public int compareTo(MyNumberWritable o) {
        MyLongWritable other = (MyLongWritable) o;
        MyLongWritable first = this;
        return first.lng.compareTo(other.lng);
    }

    @Override
    public String toString() {
        return this.lng.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lng == null) ? 0 : lng.hashCode());
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
        MyLongWritable other = (MyLongWritable) obj;
        if (lng == null) {
            if (other.lng != null)
                return false;
        } else if (!lng.equals(other.lng))
            return false;
        return true;
    }

}
