package com.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.rmi.server.RMIClassLoader;
import java.util.Arrays;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2 {
    public static class Job2Mapper extends Mapper <LongWritable, Text, Text, IntWritable> {
        protected void map(LongWritable key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            String l = value.toString();
            String[] names = l.split(" ");
            set.addAll(Arrays.asList(names));
            for(String n1 : set){
                for(String n2 : set){
                    if(n1.equals(n2)) continue;
                    else context.write(new Text(name + "," + n2), new IntWritable(1));
                }
            }
        }
    }

    public static class Job2Reducer extends Reducer <Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cnt = 0;
            for(IntWritable v : values) cnt += i.get();
            context.write(key, IntWritable.get(cnt));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"job2");
        job.setJarByClass(Job2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}