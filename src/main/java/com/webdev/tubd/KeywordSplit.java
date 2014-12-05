package com.webdev.tubd;
/**
 * Created by henriezhang on 2014/8/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KeywordSplit {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar topicsplit-1.0.jar com.webdev.tubd.KeywordSplit <in_path> <out_path>");
            System.exit(2);
        }
        String inPath = otherArgs[0];
        String outPath = otherArgs[1];

        Job job = new Job(conf, "TopicSplitPvUv");
        job.setJarByClass(KeywordSplit.class);
        job.setMapperClass(TopicSplitMapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(0);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定输入路径
        Path uPath = new Path(inPath);
        FileInputFormat.addInputPath(job, uPath);

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        // 如果输出路径已经存在则清除之
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TopicSplitMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\u0001");
            // 判断字段数个数是否合法
            if (fields.length < 6) {
                return;
            }
            String domain = fields[0];
            String url = fields[1];
            String pv = fields[2];
            String uv = fields[3];
            String keywordStr = fields[4];
            String keywords[] = keywordStr.split(",| |\t|;|(，)|(；)");

            for(int i=0; i<keywords.length; i++) {
                String kw = keywords[i].trim();
                if(!kw.equals("")) {
                    context.write(new Text(kw), new Text(domain + "\t" + url + "\t" + pv + "\t" + uv));
                }
            }
        }
    }
}