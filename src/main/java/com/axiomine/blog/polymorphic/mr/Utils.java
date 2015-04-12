package com.axiomine.blog.polymorphic.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Utils {
    public static Job getMapOnlyJob(Class jobClass,
            Class<? extends Mapper> mapperClass,
            Class<? extends WritableComparable> outputKeyClass,
            Class<? extends Writable> outputValueClass, String inputFilePath,
            String outputFilePath) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(jobClass);
        job.setMapperClass(mapperClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        job.setNumReduceTasks(0);
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(outputFilePath)))
            fs.delete(new Path(outputFilePath));
        return job;
    }

    public static Job getMapReduceJob(Class jobClass,
            Class<? extends Mapper> mapperClass,
            Class<? extends Reducer> reducerClass,
            Class<? extends WritableComparable> mapOutputKeyClass,
            Class<? extends Writable> mapOutputValueClass, 
            Class<? extends Writable> outputKeyClass,
            Class<? extends Writable> outputValueClass,
            int noOfReducers,
            String inputFilePath,
            String outputFilePath) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(jobClass);
        
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        
        job.setNumReduceTasks(noOfReducers);
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(outputFilePath)))
            fs.delete(new Path(outputFilePath));
        return job;
    }

    
}
