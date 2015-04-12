package com.axiomine.blog.polymorphic.mr;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class PolymorphicMRJobTest {
    private Mapper<LongWritable, Text, MyNumberWritable, Text> mapperLong;
    private Mapper<LongWritable, Text, MyNumberWritable, Text> mapperDouble;
    private Mapper<LongWritable, Text, MyNumberWritable, Text> mapperDoubleDouble;

    private PolymorphicReducer polymorphicReducer;

    private MapDriver<LongWritable, Text, MyNumberWritable, Text> mapDriverLong;
    private MapDriver<LongWritable, Text, MyNumberWritable, Text> mapDriverDouble;
    private MapDriver<LongWritable, Text, MyNumberWritable, Text> mapDriverDoubleDouble;

    private ReduceDriver<MyNumberWritable, Text, MyNumberWritable, Text> reduceDriverLong;
    private ReduceDriver<MyNumberWritable, Text, MyNumberWritable, Text> reduceDriverDouble;    

    private MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text> mapReduceDriverLong;
    private MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text> mapReduceDriverDouble;
    private MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text> mapReduceDriverDoubleDouble;

    @Before
    public void setUp() {
	mapperLong = new MyMappers.LongKeyTextValueMapper();
	mapperDouble = new MyMappers.DoubleKeyTextMapper();
	mapperDoubleDouble = new MyMappers.DoubleKeyDoubleTextMapper();

	polymorphicReducer = new PolymorphicReducer();

	/*
	 * Mapper's are polymorphic in signature but job definitions need to
	 * explicitly specify the actual mapper output key and output value
	 */
	mapDriverLong = new MapDriver<LongWritable, Text, MyNumberWritable, Text>(
		mapperLong);
	mapDriverDouble = new MapDriver<LongWritable, Text, MyNumberWritable, Text>(
		mapperDoubleDouble);

	mapDriverDoubleDouble = new MapDriver<LongWritable, Text, MyNumberWritable, Text>(
		mapperDouble);

	/* Reducers are fully Polymorphic */
	reduceDriverLong = new ReduceDriver<MyNumberWritable, Text, MyNumberWritable, Text>(
		polymorphicReducer);
	reduceDriverDouble = new ReduceDriver<MyNumberWritable, Text, MyNumberWritable, Text>(
		polymorphicReducer);

	mapReduceDriverLong = new MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text>(
		mapperLong, polymorphicReducer);
	mapReduceDriverDouble = new MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text>(
		mapperDouble, polymorphicReducer);
	mapReduceDriverDoubleDouble = new MapReduceDriver<LongWritable, Text, MyNumberWritable, Text, MyNumberWritable, Text>(
		mapperDouble, polymorphicReducer);

    }

    /* Test Map Only job */
    @Test
    public void testMapOnlyLong() {
	mapDriverLong.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("reduce"))
		.withOutput(new MyLongWritable(1), new Text("map"))
		.withOutput(new MyLongWritable(2), new Text("reduce"));

    }

    /* Test Map Only job */
    @Test
    public void testMapOnlyDouble() {
	mapDriverDouble.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("reduce"))
		.withOutput(new MyDoubleWritable(1), new Text("map"))
		.withOutput(new MyDoubleWritable(2), new Text("reduce"));

    }

    /* Test Map Only job */
    @Test
    public void testMapOnlyDoubleDouble() {
	mapDriverDoubleDouble.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("reduce"))
		.withOutput(new MyDoubleWritable(1), new Text("map,map"))
		.withOutput(new MyDoubleWritable(2), new Text("reduce,reduce"));

    }

    /* Test Reducer Only */
    @Test
    public void testReducerLong() {
	List<Text> lst1 = new ArrayList<Text>();
	lst1.add(new Text("map1"));
	lst1.add(new Text("map2"));

	List<Text> lst2 = new ArrayList<Text>();
	lst2.add(new Text("reduce1"));
	lst2.add(new Text("reduce2"));

	reduceDriverLong.withInput(new MyLongWritable(1), lst1)
		.withInput(new MyLongWritable(1), lst2)
		.withOutput(new MyLongWritable(1), new Text("map1"))
		.withOutput(new MyLongWritable(1), new Text("map2"))
		.withOutput(new MyLongWritable(2), new Text("reduce1"))
		.withOutput(new MyLongWritable(3), new Text("reduce2"));

    }

    /* Test Reducer Only */
    @Test
    public void testReducerDouble() {
	List<Text> lst1 = new ArrayList<Text>();
	lst1.add(new Text("map1"));
	lst1.add(new Text("map2"));

	List<Text> lst2 = new ArrayList<Text>();
	lst2.add(new Text("reduce1"));
	lst2.add(new Text("reduce2"));

	reduceDriverDouble.withInput(new MyDoubleWritable(1), lst1)
		.withInput(new MyDoubleWritable(1), lst2)
		.withOutput(new MyDoubleWritable(1), new Text("map1"))
		.withOutput(new MyDoubleWritable(1), new Text("map2"))
		.withOutput(new MyDoubleWritable(2), new Text("reduce1"))
		.withOutput(new MyDoubleWritable(3), new Text("reduce2"));

    }

    /* Test Reducer Only */

    /* Test MapReduce Job */
    @Test
    public void testMapReduceLong() {
	this.mapReduceDriverLong
		.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("map"))
		.withInput(new LongWritable(3), new Text("reduce"))
		.withOutput(new MyLongWritable(1), new Text("map"))
		.withOutput(new MyLongWritable(2), new Text("map"))
		.withOutput(new MyLongWritable(3), new Text("reduce"));
    }

    /* Test MapReduce Job */
    @Test
    public void testMapReduceDouble() {
	this.mapReduceDriverDouble
		.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("map"))
		.withInput(new LongWritable(3), new Text("reduce"))
		.withOutput(new MyDoubleWritable(1), new Text("map"))
		.withOutput(new MyDoubleWritable(2), new Text("map"))
		.withOutput(new MyDoubleWritable(3), new Text("reduce"));
    }
    /* Test MapReduce Job */
    @Test
    public void testMapReduceDoubleDouble() {
	this.mapReduceDriverDoubleDouble
		.withInput(new LongWritable(1), new Text("map"))
		.withInput(new LongWritable(2), new Text("map"))
		.withInput(new LongWritable(3), new Text("reduce"))
		.withOutput(new MyDoubleWritable(1), new MyDoubleText("map"))
		.withOutput(new MyDoubleWritable(2), new MyDoubleText("map"))
		.withOutput(new MyDoubleWritable(3), new MyDoubleText("reduce"));
    }

}
