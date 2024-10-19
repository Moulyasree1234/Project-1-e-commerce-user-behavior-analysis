package com.ecommerce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EcommerceAnalysisJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Task 1: Identifying Most Engaged Users
        Job job1 = Job.getInstance(conf, "User Activity Count");
        job1.setJarByClass(EcommerceAnalysisJob.class);
        job1.setMapperClass(UserActivityMapper.class);
        job1.setReducerClass(UserActivityReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));  // user_activity.csv
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/task1")); // Output for job 1
        job1.waitForCompletion(true);

        // Task 2: Analyzing User Purchasing Behavior Based on Time of Day
        Job job2 = Job.getInstance(conf, "Purchase Hour Analysis");
        job2.setJarByClass(EcommerceAnalysisJob.class);
        job2.setMapperClass(PurchaseHourMapper.class);
        job2.setReducerClass(PurchaseHourReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));  // transactions.csv
        FileOutputFormat.setOutputPath(job2, new Path(args[3] + "/task2")); // Output for job 2
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
