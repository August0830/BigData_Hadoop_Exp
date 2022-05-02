package com.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
    public void combine(Text key, Iterable<Text> values, Context context)
        throws IOException , InterruptedException
    {
        int sum = 0;
        for(Text val : values)
            sum += Integer.parseInt(val.toString());
        String result=Integer.toString(sum);
        int SpilitIndex=key.toString().indexOf(",");
        Text keyout = new Text();
        Text valueout = new Text();
        keyout.set(key.toString().substring(0, SpilitIndex));
        valueout.set(key.toString().substring(SpilitIndex+1)+":"+result);
        context.write(keyout, valueout);
    }
}
