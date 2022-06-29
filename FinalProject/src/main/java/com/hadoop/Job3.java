package com.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3 {
    public static class Job3Mapper extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            if(line != null && line.length() != 0)
            {
                int SplitIndex=line.indexOf(",");
                String name1=line.substring(0, SplitIndex);
                int SplitIndex2=line.indexOf("\t");
                String name2=line.substring(SplitIndex+1, SplitIndex2);
                String frequency=line.substring((SplitIndex2+1));
                context.write(new Text(name1), new Text(name2+","+frequency));
            }
        }
    }

    public static class Job3Reducer extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            double sum=0.0;
            String output="";
            ArrayList<String> s = new ArrayList<String>();
            for(Text val:values)
            {
                s.add(val.toString());
            }
            for(int i=0;i<s.size();i++)
            {
                int SplitIndex=s.get(i).indexOf(",");
                double frequency=Double.parseDouble(s.get(i).substring(SplitIndex+1));
                sum+=frequency;
            }
            for(int i=0;i<s.size();i++)
            {
                int SplitIndex=s.get(i).indexOf(",");
                String name=s.get(i).substring(0, SplitIndex);
                double frequency=Double.parseDouble(s.get(i).substring(SplitIndex+1));
                double p=frequency/sum;
                output+=name+","+String.valueOf(p)+"|";
            }
            context.write(key, new Text(output.substring(0, output.length()-1)));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf,"job3");
        job.setJarByClass(Job3.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job3Mapper.class);
        job.setReducerClass(Job3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}