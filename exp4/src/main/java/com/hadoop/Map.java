package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper <LongWritable, Text, IntWritable, Text>{

    ArrayList<ArrayList<Double>> centers = null;

    protected void setup(Context context) throws IOException, InterruptedException { //get centers
        centers = Utils.getCenters(context.getConfiguration().get("centersPath"),false);
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<Double> p = Utils.textToArrayForDataFile(value); //current data point
        double minDis=Double.MAX_VALUE;
        int index=-1;
        for(int i=0; i<centers.size(); i++) //find the nearest center
        {
            double dis=0;
            for(int j=0; j<p.size(); j++)
            {
                dis+=Math.pow(p.get(j)-centers.get(i).get(j), 2);
            }
            if(dis<minDis)
            {
                minDis=dis;
                index=i;
            }
        }
        context.write(new IntWritable(index), value);
    }
}