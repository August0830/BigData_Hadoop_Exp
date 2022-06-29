package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4_phase3 {
    public static class Phase3Mapper extends Mapper <Object, Text, DoubleWritable, Text> {
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line=value.toString();
            if(line != null && line.length() != 0)
            {
                int SplitIndex=line.indexOf("\t");
                String name=line.substring(0, SplitIndex);
                int SplitIndex2=line.indexOf("#");
                double pr=Double.parseDouble(line.substring(SplitIndex+1, SplitIndex2));
                context.write(new DoubleWritable(pr), new Text(name));
            }
        }
    }

    public static class Phase3Reducer extends Reducer <DoubleWritable, Text, DoubleWritable, Text> {
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            for(Text val:values)
            {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"job4_phase3");
        job.setJarByClass(Job4_phase3.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Phase3Mapper.class);
        job.setReducerClass(Phase3Reducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}