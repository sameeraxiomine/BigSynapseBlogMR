package com.axiomine.blog.polymorphic.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class MyDoubleText extends Text {
    
    public MyDoubleText(){
	
    }
    /**
     * Construct from a string.
     */
    public MyDoubleText(String string) {
	set(string + "," + string);
    }

    /** Construct from another text. */
    public MyDoubleText(Text utf8) {
	set(utf8.toString() + "," + utf8.toString());
    }

    /**
     * Construct from a byte array.
     */
    public MyDoubleText(byte[] utf8) {
	set(new String(utf8) + "," + new String(utf8));
    }

}
