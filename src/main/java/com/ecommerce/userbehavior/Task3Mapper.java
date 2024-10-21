package com.ecommerce.userbehavior;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Task3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        
        if (fields.length == 7) {  // Ensure the correct number of fields
            String productCategory = fields[2];  // Extract ProductCategory
            String timestamp = fields[6];  // Extract TransactionTimestamp

            try {
                // Parse the timestamp and extract the hour
                LocalDateTime transactionDate = LocalDateTime.parse(timestamp, formatter);
                int hour = transactionDate.getHour();  // Extract hour

                // Emit ProductCategory as key and the hour as the value
                context.write(new Text(productCategory), new IntWritable(hour));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
