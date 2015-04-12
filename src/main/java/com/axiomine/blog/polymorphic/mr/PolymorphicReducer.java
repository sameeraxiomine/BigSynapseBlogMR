package com.axiomine.blog.polymorphic.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Both input and output keys are polymorphic
 */
public class PolymorphicReducer extends
        Reducer<MyNumberWritable, Text, MyNumberWritable, Text> {
    public void reduce(MyNumberWritable key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        for (Text t : values) {
            context.write(key, t);        }
    }
}