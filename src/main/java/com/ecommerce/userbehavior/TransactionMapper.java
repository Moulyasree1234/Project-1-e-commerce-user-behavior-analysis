package com.ecommerce.userbehavior;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text productID = new Text();
    private Text purchase = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input CSV line by comma
        String[] fields = value.toString().split(",");

        // Skip header or malformed rows
        if (fields.length < 6 || fields[0].equals("TransactionID")) {
            return;
        }

        // Emit purchases from transactions.csv
        String productIDStr = fields[3];  // Assuming ProductID is the 4th column
        String productCategory = fields[2];  // Assuming ProductCategory is the 3rd column
        String quantitySold = fields[4];  // Assuming QuantitySold is the 5th column
        String revenueGenerated = fields[5];  // Assuming RevenueGenerated is the 6th column

        productID.set(productIDStr);
        purchase.set("purchase:" + productCategory + ":" + quantitySold + ":" + revenueGenerated); 
        context.write(productID, purchase);
    }
}
