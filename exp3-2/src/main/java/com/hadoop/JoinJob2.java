package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinJob2 {
    public static class JoinMapper extends Mapper<Object, Text, NullWritable, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            String line = value.toString();
            if (line != null && line.length() != 0) {
                String[] splits = line.split("\\|");
                if (name.contains("course.tbl")) {
                    String c_key = splits[0];
                    String c_name = splits[1];
                    String c_subject = splits[2];
                    String c_hours = splits[3];
                    context.write(NullWritable.get(),
                            new Text("C" + c_key + "|" + c_name + "|" + c_subject + "|" + c_hours));
                } else if (name.contains("university.tbl")) {
                    String u_key = splits[0];
                    String u_name = splits[1];
                    String u_webpaging = splits[4];
                    context.write(NullWritable.get(), new Text("U" + u_key + "|" + u_name + "|" + u_webpaging));
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        protected void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> listC = new ArrayList<String>();
            ArrayList<String> listU = new ArrayList<String>();
            for(Text t: values){
                String val = t.toString();
                if(val.startsWith("C")){
                    String classInfo = val.substring(1);
                    listC.add(classInfo);
                }
                else if(val.startsWith("U")){
                    String uniInfo = val.substring(1);
                    listU.add(uniInfo);
                }
            }
            for(String cls:listC){
                for(String uni:listU){
                    context.write(NullWritable.get(), new Text(cls+"|"+uni));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Join job 2");
        job.setJarByClass(JoinJob2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}