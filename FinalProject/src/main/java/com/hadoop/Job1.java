package com.hadoop;

import java.io.IOException;
import java.util.*;

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
import org.apache.hadoop.util.LineReader;

public class Job1 {
    public static class Job1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        ArrayList<String> names = new ArrayList<String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Path path = new Path(context.getConfiguration().get("namesPath"));
            Configuration conf = new Configuration();
            FileSystem fileSystem = path.getFileSystem(conf);
            FSDataInputStream fsis = fileSystem.open(path);
            LineReader lineReader = new LineReader(fsis, conf);

            Text line = new Text();
            while (lineReader.readLine(line) > 0) {
                names.add(line.toString());
            }
            lineReader.close();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String r = "";
            for (int i = 0; i < names.size(); ++i) {
                String s = names.get(i);
                int slen = s.length();
                int j = 0;
                while ((j = line.indexOf(s, j)) != -1) {
                    r = r + s + " ";
                    j = j + slen;
                }
            }
            if (r.length() > 0) {
                r = r.substring(0, r.length() - 1);
                context.write(new Text(r), NullWritable.get());
            }
        }
    }

    public static class Job1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void run(String novelPath, String namesPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        conf.set("namesPath", namesPath);
        Job job = new Job(conf, "job1");
        job.setJarByClass(Job1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(novelPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String novelPath = args[0] + "/xiyouji_sample";
        String namesPath = args[0] + "/xiyouji_name_list.txt";
        conf.set("namesPath", namesPath);
        Job job = new Job(conf, "job1");
        job.setJarByClass(Job1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job1Mapper.class);
        job.setReducerClass(Job1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(novelPath));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}