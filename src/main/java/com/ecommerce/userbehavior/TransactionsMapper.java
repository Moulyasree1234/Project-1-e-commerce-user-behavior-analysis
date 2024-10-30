package com.ecommerce.userbehavior;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text productID = new Text();
    private Text purchase = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Skip header or malformed rows
        if (fields.length < 6 || fields[0].equals("TransactionID")) {
            return;
        }

        String productIDStr = fields[3];
        String productCategory = fields[2];
        String quantitySold = fields[4];
        String revenueGenerated = fields[5];

        productID.set(productIDStr);
        purchase.set("purchase:" + productCategory + ":" + quantitySold + ":" + revenueGenerated);
        context.write(productID, purchase);
    }
}



