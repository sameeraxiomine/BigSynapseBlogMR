package com.axiomine.blog.polymorphic.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyDoubleWritable extends MyNumberWritable {
    public Double dbl = 0.0d;

    /*
     * Obligatary NoArgument Constructor so Reducer side and create Reduce side
     * key instance via reflection
     */
    public MyDoubleWritable() {

    }

    public MyDoubleWritable(double myDbl) {
        dbl = myDbl;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(dbl);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbl = in.readDouble();
    }

    @Override
    public int compareTo(MyNumberWritable o) {
        MyDoubleWritable other = (MyDoubleWritable) o;
        MyDoubleWritable first = this;
        return first.dbl.compareTo(other.dbl);
    }

    @Override
    public String toString() {
        return this.dbl.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dbl == null) ? 0 : dbl.hashCode());
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
        MyDoubleWritable other = (MyDoubleWritable) obj;
        if (dbl == null) {
            if (other.dbl != null)
                return false;
        } else if (!dbl.equals(other.dbl))
            return false;
        return true;
    }

}
