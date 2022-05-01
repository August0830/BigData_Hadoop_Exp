package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException , InterruptedException
    {
        String result = new String();
        double sum = 0;
        int cnt = 0;
        for(Text val : values)
        {
            result += val.toString() + ";";
            int SpilitIndex=val.toString().indexOf(":");
            sum += Integer.parseInt(val.toString().substring(SpilitIndex+1));
            cnt += 1;
        }
        result = String.valueOf(sum/cnt) + " " + result;
        Text valueout = new Text();
        valueout.set(result);
        context.write(key, valueout);
    }
}