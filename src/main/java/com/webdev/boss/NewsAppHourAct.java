package com.webdev.boss;
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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Vector;

public class NewsAppHourAct {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.err.println(otherArgs.length + ":" + otherArgs.toString());
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar BossUtils.jar com.webdev.tubd.NewsAppHourAct <in_path> <sdate> <edate> <out_path> [reduceNum]");
            System.exit(2);
        }
        String inPath = otherArgs[0];
        String sDate = otherArgs[1];
        String eDate = otherArgs[2];
        String outPath = otherArgs[3];
        int reduceNum = 600;
        if (args.length >= 5) {
            try {
                reduceNum = Integer.parseInt(args[4]);
            } catch (Exception e) {
            }
        }

        Job job = new Job(conf, "NewsAppHourAct");
        job.setJarByClass(NewsAppHourAct.class);
        job.setMapperClass(NewsAppHourActMapper.class);
        job.setReducerClass(NewsAppHourActReducer.class);

        // the map output is IntWritable, Text
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // the reduce output is Text, Text
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(reduceNum);

        // 指定输入路径
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        Date dt1 = formatter.parse(sDate);
        Date dt2 = formatter.parse(eDate);
        Calendar cd1 = Calendar.getInstance();
        cd1.setTime(dt1);
        int endDs = Integer.parseInt(formatter.format(dt2));
        FileSystem fs = FileSystem.get(conf);
        for (int i = 1; Integer.parseInt(formatter.format(cd1.getTime())) <= endDs && i < 400; i++) {
            String tmpPath = inPath + "/ds=" + formatter.format(cd1.getTime());
            System.out.println("Check Path " + tmpPath);
            Path tPath = new Path(tmpPath);
            if (fs.exists(tPath)) {
                FileInputFormat.addInputPath(job, tPath);
                System.out.println("Exist " + tmpPath);
            } else {
                System.out.println("Not exist " + tmpPath);
            }
            cd1.add(Calendar.DATE, 1);
        }

        // 指定输出文件路径
        Path oPath = new Path(outPath);
        if (fs.exists(oPath)) {
            fs.deleteOnExit(oPath);
        }
        FileOutputFormat.setOutputPath(job, oPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class NewsAppHourActMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text inValue, Context context)
                throws IOException, InterruptedException {
            String[] fields = inValue.toString().split("\u0001");
            // 判断字段数个数是否合法
            if (fields.length < 26) {
                return;
            }

            String user = fields[0];
            StringBuilder sb = new StringBuilder();
            for (int i = 2; i < 26; i++) {
                sb.append("\u0001");
                sb.append(fields[i]);
            }

            context.write(new Text(user), new Text(sb.substring(1)));
        }
    }

    public static class NewsAppHourActReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text user, Iterable<Text> actItems, Context context)
                throws IOException, InterruptedException {
            List<String> allAct = new Vector<String>();
            for (Text item : actItems) {
                allAct.add(item.toString());
            }

            // 汇总数据
            int size = allAct.size();
            int actHours[] = new int[24];
            for (int i = 0; i < size; i++) {
                String it = allAct.get(i);
                String fields[] = it.split("\u0001");
                for (int j = 0; j < fields.length && j < 24; j++) {
                    int cnt = 0;
                    try {
                        cnt = Integer.parseInt(fields[j]);
                    } catch (Exception ex) {
                    }
                    actHours[j] += cnt;
                }
            }

            // 计算均值、最大值等
            int maxHour = 0, maxValue = 0, all = 0;
            for (int i = 0; i < 24; i++) {
                all += actHours[i];
                if (actHours[i] > maxValue) {
                    maxHour = i;
                    maxValue = actHours[i];
                }
            }

            // 拼装结果值
            StringBuilder sb = new StringBuilder();
            sb.append(maxHour);
            sb.append("\t");
            sb.append(maxValue);
            sb.append("\t");
            sb.append(all);
            context.write(user, new Text(sb.toString()));
        }
    }
}