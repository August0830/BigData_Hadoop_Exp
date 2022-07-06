package com.hadoop;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2 {
    public static class Job2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> sameNames = new HashMap<String, String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            sameNames.put("唐僧", "唐僧");
            sameNames.put("唐三藏", "唐僧");
            sameNames.put("陈玄奘", "唐僧");
            sameNames.put("玄奘", "唐僧");
            sameNames.put("唐长老", "唐僧");
            sameNames.put("金蝉子", "唐僧");
            sameNames.put("旃檀功德佛", "唐僧");
            sameNames.put("江流儿", "唐僧");
            sameNames.put("江流", "唐僧");

            sameNames.put("孙悟空", "孙悟空");
            sameNames.put("悟空", "孙悟空");
            sameNames.put("齐天大圣", "孙悟空");
            sameNames.put("美猴王", "孙悟空");
            sameNames.put("猴王", "孙悟空");
            sameNames.put("斗战胜佛", "孙悟空");
            sameNames.put("孙行者", "孙悟空");
            sameNames.put("心猿", "孙悟空");
            sameNames.put("金公", "孙悟空");

            sameNames.put("猪八戒", "猪八戒");
            sameNames.put("猪悟能", "猪八戒");
            sameNames.put("悟能", "猪八戒");
            sameNames.put("八戒", "猪八戒");
            sameNames.put("猪刚鬣", "猪八戒");
            sameNames.put("老猪", "猪八戒");
            sameNames.put("净坛使者", "猪八戒");
            sameNames.put("天蓬元帅", "猪八戒");
            sameNames.put("木母", "猪八戒");

            sameNames.put("沙僧", "沙僧");
            sameNames.put("沙和尚", "沙僧");
            sameNames.put("沙悟净", "沙僧");
            sameNames.put("悟净", "沙僧");
            sameNames.put("金身罗汉", "沙僧");
            sameNames.put("卷帘大将", "沙僧");
            sameNames.put("刀圭", "沙僧");

            sameNames.put("白龙马", "白龙马");
            sameNames.put("小白龙", "白龙马");
            sameNames.put("白马", "白龙马");
            sameNames.put("八部天龙马", "白龙马");

            sameNames.put("如来佛祖", "如来佛祖");
            sameNames.put("如来", "如来佛祖");

            sameNames.put("观音菩萨", "观音菩萨");
            sameNames.put("观音", "观音菩萨");
            sameNames.put("观世音菩萨", "观音菩萨");
            sameNames.put("观世音", "观音菩萨");

            sameNames.put("玉帝", "玉帝");
            sameNames.put("玉皇大帝", "玉帝");

            sameNames.put("陈萼", "陈萼");
            sameNames.put("光蕊", "陈萼");

            sameNames.put("唐太宗", "唐太宗");
            sameNames.put("李世民", "唐太宗");

            sameNames.put("殷温娇", "殷温娇");
            sameNames.put("满堂娇", "殷温娇");

            sameNames.put("罗刹女", "罗刹女");
            sameNames.put("铁扇公主", "罗刹女");

            sameNames.put("圣婴大王", "圣婴大王");
            sameNames.put("红孩儿", "圣婴大王");
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String l = value.toString();
            String[] names = l.split(" ");
            for (int i = 0; i < names.length; ++i) {
                String n1 = names[i];
                for (int j = 0; j < names.length; ++j) {
                    String n2 = names[j];
                    if (n1.equals(n2))
                        continue;
                    else if (sameNames.get(n1) != null && sameNames.get(n1) == sameNames.get(n2))
                        continue;
                    else {
                        if (sameNames.get(n1) != null)
                            n1 = sameNames.get(n1).toString();
                        if (sameNames.get(n2) != null)
                            n2 = sameNames.get(n2).toString();
                        context.write(new Text(n1 + "," + n2), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable v : values)
                cnt += v.get();
            context.write(key, new IntWritable(cnt));
        }
    }

    public static void run(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "job2");
        job.setJarByClass(Job2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "job2");
        job.setJarByClass(Job2.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(Job2Mapper.class);
        job.setReducerClass(Job2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}