package com.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.rmi.server.RMIClassLoader;
import java.util.*;

import javax.naming.Context;
import javax.xml.stream.events.Namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.ansj.demo;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

public class Job1 {
    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        protected void setup(Context context) throws IOException, InterruptedException {
            String namesPath = context.getConfiguration().get("namesPath");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new FileReader(namesPath));
            String name;
            while ((name = br.readLine()) != null) {
                DicLibrary.insert(Diclibrary.DEFAULT, name);
            }
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Result res = DicAnalysis.parse(line);
            List<Term> terms = res.getTerms();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < terms.size(); ++i) {
                String w = terms.get(i).getName();
                String natureStr = terms.get(i).getNatureStr();
                if (natureStr.equals("userDefine")) {
                    sb.append(w + " ");
                }
            }
            String r = sb.length() > 0 ? sb.toString().substring(0, sb.length() - 1) : "";
            context.write(new Text(r), NullWritable.get());
        }
    }

    public static class Job1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "job1");
        job.setJarByClass(Job1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String novelPath = args[0] + "/xiyouji_sample";
        String namesPath = args[0] + "/xiyouji_name_list.txt";
        conf.set("novelPath", novelPath);
        conf.set("namesPath", namesPath);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}