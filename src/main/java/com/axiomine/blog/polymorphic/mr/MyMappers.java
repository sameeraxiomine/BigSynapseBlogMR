package com.axiomine.blog.polymorphic.mr;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * List of custom mappers used to illustrate polymorphism
 */
public class MyMappers {

    /*
     * Output Key is polymorphic 
     * Declared - NumberWritable
     * Actual - MyLongWritable
     */
    public static class LongKeyTextValueMapper extends
            Mapper<LongWritable, Text, MyNumberWritable, Text> {
        public void map(LongWritable key, Text word, Context context)
                throws IOException, InterruptedException {
            context.write(new MyLongWritable(key.get()), word);
        }
    }

    /*
     * Output Key is polymorphic 
     * Declared - NumberWritable
     * Actual - MyDoubleWritable
     */
    public static class DoubleKeyTextMapper extends
            Mapper<LongWritable, Text, MyNumberWritable, Text> {
        public void map(LongWritable key, Text word, Context context)
                throws IOException, InterruptedException {
            context.write(new MyDoubleWritable(new Double(key.get())), word);
        }
    }

    /*
     * Output Key/Value is polymorphic 
     */
    public static class DoubleKeyDoubleTextMapper extends
            Mapper<LongWritable, Text, MyNumberWritable, Text> {
        public void map(LongWritable key, Text word, Context context)
                throws IOException, InterruptedException {
            context.write(new MyDoubleWritable(new Double(key.get())), new MyDoubleText(word));
        }
    }

}