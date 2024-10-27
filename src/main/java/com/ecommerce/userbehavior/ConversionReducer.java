package com.ecommerce.userbehavior;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConversionReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int interactionCount = 0;
        int purchaseCount = 0;
        double totalRevenue = 0.0;
        String productCategory = "";

        // Loop through values for a given productID
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts[0].equals("interaction")) {
                interactionCount++;
            } else if (parts[0].equals("purchase")) {
                purchaseCount++;
                productCategory = parts[1];  // Extracting product category from purchase
                totalRevenue += Double.parseDouble(parts[3]);  // Adding up the revenue
            }
        }

        // Calculate conversion rate
        double conversionRate = (interactionCount == 0) ? 0 : (double) purchaseCount / interactionCount;

        // Output: ProductID, ProductCategory, ConversionRate, TotalRevenue
        context.write(key, new Text(productCategory + ", ConversionRate: " + conversionRate + ", TotalRevenue: " + totalRevenue));
    }
}
