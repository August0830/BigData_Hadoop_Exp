package com.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4_phase1 {
    public static class Phase1Mapper extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line=value.toString();
            if(line != null && line.length() != 0)
            {
                int SplitIndex=line.indexOf("\t");
                String name=line.substring(0, SplitIndex);
                String list=line.substring(SplitIndex+1);
                context.write(new Text(name), new Text("1#"+list));
            }
        }
    }

    public static class Phase1Reducer extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
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
        Job job = new Job(conf,"job4_phase1");
        job.setJarByClass(Job4_phase1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Phase1Mapper.class);
        job.setReducerClass(Phase1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
