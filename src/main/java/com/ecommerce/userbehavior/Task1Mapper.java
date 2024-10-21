package com.ecommerce.userbehavior;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Task1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Weights for each ActivityType
    private static final int BROWSING_WEIGHT = 1;
    private static final int ADD_TO_CART_WEIGHT = 3;
    private static final int PURCHASE_WEIGHT = 5;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String userId = fields[1];  // Assuming UserID is the first field
        String activityType = fields[2];  // Assuming ActivityType is the second field

        int weight = 0;
        switch (activityType.toLowerCase()) {
            case "browse":
                weight = BROWSING_WEIGHT;
                break;
            case "add_to_cart":
                weight = ADD_TO_CART_WEIGHT;
                break;
            case "purchase":
                weight = PURCHASE_WEIGHT;
                break;
        }

        // Emit UserID with the weighted activity score
        context.write(new Text(userId), new IntWritable(weight));
    }
}
