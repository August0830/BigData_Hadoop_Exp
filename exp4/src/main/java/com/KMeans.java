package com;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.Map;
import com.hadoop.Reduce;
import com.hadoop.Utils;

import java.io.IOException;

public class KMeans {
    public static void run(String centerpath, String datasetpath, String outputpath, boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf  = new Configuration();
        conf.set("centersPath", centerpath);
        Job job = new Job(conf, "kmeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(runReduce)
        {
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        FileInputFormat.addInputPath(job, new Path(datasetpath));
        FileOutputFormat.setOutputPath(job, new Path(outputpath));
        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String centerpath = args[0] + "/initial_centers";
        String datasetpath = args[0] + "/dataset.data";
        String outputpath = args[1];
        while(true)
        {
            run(centerpath, datasetpath, outputpath, true);
            if(Utils.compareCenters(centerpath, outputpath))
            {
                run(centerpath, datasetpath, outputpath, false);
                break;
            }
        }
    }
}
