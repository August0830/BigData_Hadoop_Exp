package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinJob1 {
    public static class JoinMapper extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String name = fileSplit.getPath().getName();
            String line = value.toString();
            if(line != null && line.length() != 0)
            {
                String[] split=line.split("\\|");
                if(name.contains("university.tbl"))
                {
                    String u_key=split[0];
                    String u_name=split[1];
                    String u_web=split[4];
                    String join_key=split[2];
                    context.write(new Text(join_key), new Text("U"+u_key+"|"+u_name+"|"+u_web));
                }
                else if(name.contains("country.tbl"))
                {
                    String join_key=split[0];
                    String n_name=split[1];
                    context.write(new Text(join_key), new Text("C"+n_name));
                }
                else;
            }
            else;
        }
    }

    public static class JoinReducer extends Reducer <Text, Text, NullWritable, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            ArrayList<String> tb1 = new ArrayList<String>();
            ArrayList<String> tb2 = new ArrayList<String>();
            for(Text val: values)
            {
                if(val.toString().startsWith("U")==true)
                {
                    String val1=val.toString().substring(1);
                    tb1.add(val1);
                }
                else if(val.toString().startsWith("C")==true)
                {
                    String val2=val.toString().substring(1);
                    tb2.add(val2);
                }
            }
            for(String uni: tb1)
            {
                for(String cty: tb2)
                {
                    context.write(NullWritable.get(), new Text(uni+"|"+cty));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"join job1");
        job.setJarByClass(JoinJob1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
