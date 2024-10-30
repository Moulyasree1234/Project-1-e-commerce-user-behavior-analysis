package com.ecommerce.userbehavior;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text productID = new Text();
    private Text activity = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Skip header or malformed rows
        if (fields.length < 4 || fields[0].equals("LogID")) {
            return;
        }

        String productIDStr = fields[3];
        String activityType = fields[2];

        productID.set(productIDStr);
        activity.set("interaction:" + activityType);
        context.write(productID, activity);
    }
}



