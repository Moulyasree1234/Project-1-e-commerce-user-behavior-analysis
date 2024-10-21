package com.ecommerce.userbehavior;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class Task1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // PriorityQueue to keep track of top 10 users by engagement score
    private PriorityQueue<UserScore> topUsersQueue;
    private static final int TOP_USERS_COUNT = 10;

    @Override
    protected void setup(Context context) {
        topUsersQueue = new PriorityQueue<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalScore = 0;
        for (IntWritable value : values) {
            totalScore += value.get();
        }

        // Add the user and their total engagement score to the priority queue
        topUsersQueue.offer(new UserScore(key.toString(), totalScore));

        // Remove the lowest score if we have more than 10 users
        if (topUsersQueue.size() > TOP_USERS_COUNT) {
            topUsersQueue.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the top 10 users with the highest engagement score
        while (!topUsersQueue.isEmpty()) {
            UserScore entry = topUsersQueue.poll();
            context.write(new Text(entry.userId), new IntWritable(entry.score));
        }
    }

    // Custom class to store user and score, with comparable for PriorityQueue
    private static class UserScore implements Comparable<UserScore> {
        String userId;
        int score;

        public UserScore(String userId, int score) {
            this.userId = userId;
            this.score = score;
        }

        @Override
        public int compareTo(UserScore other) {
            return Integer.compare(this.score, other.score);
        }
    }
}
