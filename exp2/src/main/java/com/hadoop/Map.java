package com.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map extends Mapper<Object,Text,Text,Text>  {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        StringTokenizer str = new StringTokenizer(value.toString());
        Text keyInfo = new Text();
        Text valueInfo = new Text();
        while(str.hasMoreTokens()){
            //how to move prefix of filename
            keyInfo.set(str.nextToken()+":"+split.getPath().toString());
            valueInfo.set("1");
            context.write(keyInfo,valueInfo);
        }
    }
}