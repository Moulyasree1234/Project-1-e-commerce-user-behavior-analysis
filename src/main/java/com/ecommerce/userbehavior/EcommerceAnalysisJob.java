package com.ecommerce.userbehavior;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EcommerceAnalysisJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Usage: EcommerceAnalysisJob <input path for Task 1> <output path for Task 1> " +
                    "<input path for Task 2 user_activity> <input path for Task 2 transactions> <output path for Task 2> " +
                    "<input path for Task 3> <output path for Task 3>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Task 1: Identifying Most Engaged Users
        Job job1 = Job.getInstance(conf, "User Activity Count");
        job1.setJarByClass(EcommerceAnalysisJob.class);
        job1.setMapperClass(Task1Mapper.class);
        job1.setReducerClass(Task1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));  // Input path for user activity
        FileOutputFormat.setOutputPath(job1, new Path(args[1])); // Output path for Task 1
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Task 2: Product Purchase Conversion Rate
        Job job2 = Job.getInstance(conf, "Product Purchase Conversion Rate");
        job2.setJarByClass(EcommerceAnalysisJob.class);
        
        job2.setReducerClass(ConversionReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, ActivityMapper.class); // user_activity.csv
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, TransactionsMapper.class); // transactions.csv
        
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));  // Output path for Task 2

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        // Task 3: Analyzing User Purchasing Behavior Based on Time of Day
        Job job3 = Job.getInstance(conf, "User Purchasing Behavior by Hour");
        job3.setJarByClass(EcommerceAnalysisJob.class);
        job3.setMapperClass(Task3Mapper.class);
        job3.setReducerClass(Task3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job3, new Path(args[5]));  // Input path for transactions
        FileOutputFormat.setOutputPath(job3, new Path(args[6])); // Output path for Task 3

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}