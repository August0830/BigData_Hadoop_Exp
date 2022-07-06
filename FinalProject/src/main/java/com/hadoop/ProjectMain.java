package com.hadoop;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.hadoop.Job3.Job3Mapper;
import com.hadoop.Job3.Job3Reducer;
import com.hadoop.Job4_phase1.Phase1Mapper;
import com.hadoop.Job4_phase1.Phase1Reducer;
import com.hadoop.Job4_phase2.Phase2Mapper;
import com.hadoop.Job4_phase2.Phase2Reducer;
import com.hadoop.Job4_phase3.DescSort;
import com.hadoop.Job4_phase3.Phase3Mapper;
import com.hadoop.Job4_phase3.Phase3Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tools.ant.taskdefs.Input;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class ProjectMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String novelPath = args[0] + "/xiyouji";
        String namesPath = args[0] + "/xiyouji_name_list.txt";
        String job1Data = args[1] + "/job1data";
        conf.set("novelPath", novelPath);
        conf.set("namesPath", namesPath);
        Job1.run(novelPath, namesPath, job1Data);

        String job2Data = args[1] + "/job2data";
        Job2.run(job1Data, job2Data);

        String job3Data = args[1] + "/job3data";
        Job job3 = new Job(conf, "job3");
        job3.setJarByClass(Job3.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapperClass(Job3Mapper.class);
        job3.setReducerClass(Job3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job2Data));
        FileOutputFormat.setOutputPath(job3, new Path(job3Data));
        job3.waitForCompletion(true);

        String job4_1Data = args[1] + "/job4_1Data_0";
        Job job4_1 = new Job(conf, "job4_phase1");
        job4_1.setJarByClass(Job4_phase1.class);
        job4_1.setInputFormatClass(TextInputFormat.class);
        job4_1.setMapperClass(Phase1Mapper.class);
        job4_1.setReducerClass(Phase1Reducer.class);
        job4_1.setOutputKeyClass(Text.class);
        job4_1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4_1, new Path(job3Data));
        FileOutputFormat.setOutputPath(job4_1, new Path(job4_1Data));
        job4_1.waitForCompletion(true);

        int loopTimes = 10;
        String InputPath = args[1] + "/job4_1Data_0";
        String OutputPath = args[1] + "/job4_2Data_0";
        String tmpPath = null;
        for (int i = 0; i < loopTimes; i++) {
            OutputPath = OutputPath.substring(0, OutputPath.length() - 1);
            OutputPath = OutputPath + String.valueOf(i);
            Job job4_2 = new Job(conf, "job4_phase2");
            job4_2.setJarByClass(Job4_phase2.class);
            job4_2.setInputFormatClass(TextInputFormat.class);
            job4_2.setMapperClass(Phase2Mapper.class);
            job4_2.setReducerClass(Phase2Reducer.class);
            job4_2.setOutputKeyClass(Text.class);
            job4_2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job4_2, new Path(InputPath));
            FileOutputFormat.setOutputPath(job4_2, new Path(OutputPath));
            tmpPath = OutputPath;// 把输出的地址改成下一次迭代的输入地址
            OutputPath = InputPath;
            InputPath = tmpPath;
            job4_2.waitForCompletion(true);
        }
        OutputPath = args[1] + "/job4_1Data_9";

        Job job4_3 = new Job(conf, "job4_phase3");
        String job4_3Data = args[1] + "/job4_3Data";
        job4_3.setJarByClass(Job4_phase3.class);
        job4_3.setSortComparatorClass(DescSort.class);
        job4_3.setInputFormatClass(TextInputFormat.class);
        job4_3.setMapperClass(Phase3Mapper.class);
        job4_3.setReducerClass(Phase3Reducer.class);
        job4_3.setOutputKeyClass(DoubleWritable.class);
        job4_3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4_3, new Path(OutputPath));
        FileOutputFormat.setOutputPath(job4_3, new Path(job4_3Data));
        System.exit(job4_3.waitForCompletion(true) ? 0 : 1);
    }

}
