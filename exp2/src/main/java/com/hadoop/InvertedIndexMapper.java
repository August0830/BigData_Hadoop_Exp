package com.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object,Text,Text,IntWritable>  {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        StringTokenizer str = new StringTokenizer(value.toString());
        Text keyInfo = new Text();
        IntWritable valueInfo = new IntWritable(1);
        while(str.hasMoreTokens()){
            //how to move prefix of filename
            keyInfo.set(str.nextToken()+","+split.getPath().toString());
            context.write(keyInfo,valueInfo);
        }
    }
}
