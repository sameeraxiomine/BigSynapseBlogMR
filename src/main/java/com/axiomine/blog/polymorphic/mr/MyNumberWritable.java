package com.axiomine.blog.polymorphic.mr;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public abstract class MyNumberWritable implements WritableComparable<MyNumberWritable> {

}