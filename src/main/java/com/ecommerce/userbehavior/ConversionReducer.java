package com.ecommerce.userbehavior;



import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConversionReducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Integer> categoryInteractionCounts = new HashMap<>();
    private Map<String, Integer> categoryPurchaseCounts = new HashMap<>();
    private Map<String, Double> categoryRevenueTotals = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int interactionCount = 0;
        int purchaseCount = 0;
        double totalRevenue = 0.0;
        String productCategory = "";

        // Process each value for a given productID
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            if (parts[0].equals("interaction")) {
                interactionCount++;
            } else if (parts[0].equals("purchase")) {
                purchaseCount++;
                productCategory = parts[1];
                totalRevenue += Double.parseDouble(parts[3]);
            }
        }

        // Aggregate results by product category
        categoryInteractionCounts.put(productCategory, categoryInteractionCounts.getOrDefault(productCategory, 0) + interactionCount);
        categoryPurchaseCounts.put(productCategory, categoryPurchaseCounts.getOrDefault(productCategory, 0) + purchaseCount);
        categoryRevenueTotals.put(productCategory, categoryRevenueTotals.getOrDefault(productCategory, 0.0) + totalRevenue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the aggregated results by category
        for (String category : categoryInteractionCounts.keySet()) {
            int totalInteractions = categoryInteractionCounts.get(category);
            int totalPurchases = categoryPurchaseCounts.get(category);
            double totalRevenue = categoryRevenueTotals.get(category);
            double conversionRate = (totalInteractions == 0) ? 0 : (double) totalPurchases / totalInteractions;

            // Output: ProductCategory, ConversionRate, TotalRevenue
            context.write(new Text(category), new Text("ConversionRate: " + conversionRate + ", TotalRevenue: " + totalRevenue));
        }
    }
}


