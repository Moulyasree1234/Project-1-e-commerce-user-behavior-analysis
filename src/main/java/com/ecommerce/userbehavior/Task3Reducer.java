package com.ecommerce.userbehavior;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Task3Reducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Integer> hourCountMap = new HashMap<>();

        // Count occurrences of each hour
        for (IntWritable value : values) {
            int hour = value.get();
            hourCountMap.put(hour, hourCountMap.getOrDefault(hour, 0) + 1);
        }

        // Find the hour with the maximum count for this product category
        int peakHour = -1;
        int maxCount = 0;
        for (Map.Entry<Integer, Integer> entry : hourCountMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                peakHour = entry.getKey();
            }
        }

        // Emit the ProductCategory and the most popular hour along with the count of purchases in that hour
        context.write(key, new Text("Most Popular Hour: " + peakHour + " with " + maxCount + " purchases"));
    }
}
