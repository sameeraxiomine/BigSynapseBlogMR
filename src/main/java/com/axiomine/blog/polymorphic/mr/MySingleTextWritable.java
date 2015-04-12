package com.axiomine.blog.polymorphic.mr;


public class MySingleTextWritable extends MyTextWritable {

    /*
     * Obligatary NoArgument Constructor so Reducer side and create Reduce side
     * key instance via reflection
     */
    public MySingleTextWritable() {

    }

    public MySingleTextWritable(String s) {
	str = s;
    }




}
