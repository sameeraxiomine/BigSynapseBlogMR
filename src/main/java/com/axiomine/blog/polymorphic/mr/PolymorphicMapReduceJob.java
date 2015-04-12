package com.axiomine.blog.polymorphic.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PolymorphicMapReduceJob {
    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String longOutputPath = args[1];        
        String doubleOutputPath = args[2];
        String doubleDoubleOutputPath = args[3];
        
        int noOfReducers = 1;
        Class jobClass = PolymorphicMapReduceJob.class;//Just needs to be from the same jar file
        Class<? extends Mapper> mapperClass = MyMappers.LongKeyTextValueMapper.class;
        Class<? extends Reducer> reducerClass = PolymorphicReducer.class;
        
        Class<? extends WritableComparable> mapOutputKeyClass = MyLongWritable.class;//Non-Polymorphic
        Class<? extends Writable> mapOutputValueClass = Text.class;
        Class<? extends Writable> outputKeyClass = MyNumberWritable.class;//Polymorphic
        Class<? extends Writable> outputValueClass = Text.class;

        Job ljob = Utils.getMapReduceJob(jobClass, mapperClass,
                reducerClass, mapOutputKeyClass, mapOutputValueClass, 
                outputKeyClass, outputValueClass,noOfReducers, inputPath, longOutputPath);
        boolean status = ljob.waitForCompletion(true);
        System.out.println("Long MR Job Status "+ status);

        mapperClass = MyMappers.DoubleKeyTextMapper.class;
        mapOutputKeyClass = MyDoubleWritable.class;//Non-Polymorphic
        //If output value showed polymorphic behaviour it would need to be specified too.
        Job djob = Utils.getMapReduceJob(jobClass, mapperClass,
                reducerClass, mapOutputKeyClass, mapOutputValueClass, 
                outputKeyClass, outputValueClass,noOfReducers, inputPath, doubleOutputPath);
        status = djob.waitForCompletion(true);
        System.out.println("Double MR Job Status "+ status);

        mapperClass = MyMappers.DoubleKeyDoubleTextMapper.class;
        mapOutputKeyClass = MyDoubleWritable.class;//Non-Polymorphic
        mapOutputValueClass = MyDoubleText.class;//Non-Polymorphic
        //If output value showed polymorphic behaviour it would need to be specified too.
        Job ddjob = Utils.getMapReduceJob(jobClass, mapperClass,
                reducerClass, mapOutputKeyClass, mapOutputValueClass, 
                outputKeyClass, outputValueClass,noOfReducers, inputPath, doubleDoubleOutputPath);
        status = ddjob.waitForCompletion(true);
        System.out.println("Double MR Job Status "+ status);

    }
}