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

public class Job4_phase2 {
    public static class Phase2Mapper extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line=value.toString();
            if(line != null && line.length() != 0)
            {
                int SplitIndex=line.indexOf("\t");
                String name=line.substring(0, SplitIndex);
                int SplitIndex2=line.indexOf("#");
                double pr=Double.parseDouble(line.substring(SplitIndex+1, SplitIndex2));
                String list=line.substring(SplitIndex2+1);
                String[] split=list.split("\\|");
                for(String val:split)
                {
                    int SplitIndex3=val.indexOf(",");
                    String val_name=val.substring(0, SplitIndex3);
                    double p=Double.parseDouble(val.substring(SplitIndex3+1));
                    double val_pr=pr*p;
                    context.write(new Text(val_name), new Text(String.valueOf(val_pr)));
                }
                context.write(new Text(name), new Text("#"+list));
            }
        }
    }

    public static class Phase2Reducer extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            double sum=0.0;
            String list="";
            for(Text val:values)
            {
                String val_str=val.toString();
                if(val_str.startsWith("#")==false)
                {
                    sum+=Double.parseDouble(val_str);
                }
                else list+=val_str;
            }
            context.write(key, new Text(String.valueOf(sum)+list));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String InputPath=args[0];
        String OutputPath=args[1];
        for(int i=1;i<31;i++)
        {
            Job job = new Job(conf,"job4_phase2");
            job.setJarByClass(Job4_phase2.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(Phase2Mapper.class);
            job.setReducerClass(Phase2Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(InputPath));
            FileOutputFormat.setOutputPath(job, new Path(OutputPath));
            InputPath=OutputPath;//把输出的地址改成下一次迭代的输入地址
            OutputPath=OutputPath+i;//把下一次的输出设置成一个新地址
            job.waitForCompletion(true);
        }
    }
}
