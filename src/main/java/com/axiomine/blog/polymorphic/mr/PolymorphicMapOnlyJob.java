package com.axiomine.blog.polymorphic.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PolymorphicMapOnlyJob {
    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String longOutputPath = args[1];        
        String doubleOutputPath = args[2];
        String doubleDoubleOutputPath = args[3];
        
        Class jobClass = PolymorphicMapOnlyJob.class;//Just needs to be from the same jar file
        Class<? extends Mapper> mapperClass = MyMappers.LongKeyTextValueMapper.class;        
        Class<? extends WritableComparable> outputKeyClass = MyNumberWritable.class;//Polymorphic
        Class<? extends Writable> outputValueClass = Text.class;//Polymorphic

        Job ljob = Utils.getMapOnlyJob(jobClass,mapperClass,outputKeyClass,outputValueClass,
                inputPath, longOutputPath);
        boolean status = ljob.waitForCompletion(true);
        System.out.println("Long Job Status "+ status);
        
        mapperClass = MyMappers.DoubleKeyTextMapper.class;
        Job djob = Utils.getMapOnlyJob(jobClass,mapperClass,outputKeyClass,outputValueClass,
                inputPath, doubleOutputPath);
        status = djob.waitForCompletion(true);
        System.out.println("Double Job Status "+ status);

        mapperClass = MyMappers.DoubleKeyDoubleTextMapper.class;
        Job ddjob = Utils.getMapOnlyJob(jobClass,mapperClass,outputKeyClass,outputValueClass,
                inputPath, doubleDoubleOutputPath);
        status = ddjob.waitForCompletion(true);
        System.out.println("DoubleDouble Job Status "+ status);

    }
}