package com.hadoop;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import com.hadoop.Job1.Job1Mapper;
import com.hadoop.Job1.Job1Reducer;
import com.hadoop.Job2.Job2Mapper;
import com.hadoop.Job2.Job2Reducer;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

public class ProjectMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        Job job1 = new Job(conf,"job1");
        job1.setJarByClass(Job1.class);
        job1.setJarByClass(Job1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(Job1Mapper.class);
        job1.setReducerClass(Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //args[0] 是数据源所在的位置
        //args[1] 是结果文件夹所在的位置
        String novelPath = args[0] + "/xiyouji_sample";
        String namesPath = args[0] + "/xiyouji_name_list.txt";
        String job1Data = args[1] + "/job1data.txt";
        //可以改 不太确定目前是什么格式 @Attention 下同
        conf.set("novelPath", novelPath);
        conf.set("namesPath", namesPath);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(job1Data));
        job1.waitForCompletion(true);

        Job job2 = new Job(conf,"job2");
        String job2Data = args[1] + "/job2data.txt";
        job2.setJarByClass(Job2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(job1Data));
        FileOutputFormat.setOutputPath(job2, new Path(job2Data));
        job2.waitForCompletion(true);

        String job3Data = args[1] + "/job3data.txt";
        Job job3 = new Job(conf,"job3");
        job3.setJarByClass(Job3.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setMapperClass(Job3Mapper.class);
        job3.setReducerClass(Job3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job2Data));
        FileOutputFormat.setOutputPath(job3, new Path(job3Data));
        job3.waitForCompletion(true);

        String job4_1Data = args[1] + "/job4_1Data.txt";
        Job job4_1 = new Job(conf,"job4_phase1");
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
        String InputPath=args[1] + "/job4_1Data.txt";
        String OutputPath=args[1] + "/job4_2Data.txt";
        String tmpPath = null;
        for(int i=0;i<loopTimes;i++)
        {
            Job job4_2 = new Job(conf,"job4_phase2");
            job4_2.setJarByClass(Job4_phase2.class);
            job4_2.setInputFormatClass(TextInputFormat.class);
            job4_2.setMapperClass(Phase2Mapper.class);
            job4_2.setReducerClass(Phase2Reducer.class);
            job4_2.setOutputKeyClass(Text.class);
            job4_2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job4_2, new Path(InputPath));
            FileOutputFormat.setOutputPath(job4_2, new Path(OutputPath));
            tmpPath = OutputPath;//把输出的地址改成下一次迭代的输入地址
            OutputPath = InputPath;
            InputPath = tmpPath;
            job4_2.waitForCompletion(true);
        }
        if(loopTimes%2==1)
            OutputPath = args[1] + "/job4_2Data.txt";
        else
            OutputPath = args[1] + "/job4_1Data.txt";

        Job job4_3 = new Job(conf,"job4_phase3");
        String job4_3Data = args[1] + "/job4_3Data.txt";
        job4_3.setJarByClass(Job4_phase3.class);
        job4_3.setSortComparatorClass(DescSort.class);
        job4_3.setInputFormatClass(TextInputFormat.class);
        job4_3.setMapperClass(Phase3Mapper.class);
        job4_3.setReducerClass(Phase3Reducer.class);
        job4_3.setOutputKeyClass(DoubleWritable.class);
        job4_3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4_3, new Path(OutputPath));
        FileOutputFormat.setOutputPath(job4_3, new Path(job4_3Data));
        System.exit(job4_3.waitForCompletion(true)?0:1);
    }
    
}
