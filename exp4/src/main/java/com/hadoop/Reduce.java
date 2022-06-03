package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
    protected void reduce(IntWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
        ArrayList<ArrayList<Double>> cluster = new ArrayList<ArrayList<Double>>();
        for(Text val:value)
        {
            ArrayList<Double> p = Utils.textToArrayForDataFile(val);
            cluster.add(p);
        }
        double[] avg = new double[cluster.get(0).size()];
        for(int i=0;i<cluster.get(0).size();i++)
        {
            double sum=0;
            for(int j=0;j<cluster.size();j++)//calculate sum for each column
            {
                sum+=cluster.get(j).get(i);
            }
            avg[i]=sum/cluster.size();
        }
        String center="";
        for(int i=0;i<avg.length;i++)
        {
            center+=String.valueOf(avg[i]);
            if(i<avg.length-1)
                center+=",";
        }
        context.write(key, new Text(center));
    }
}
