package com.junenatte.hadoop.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String friend = line.substring(0, line.indexOf(':'));
        String[] sp = line.substring(2).split(",");
        if (sp.length <= 1)
            return;
        Arrays.sort(sp);
        for (int i = 0; i < sp.length - 1; i++)
            for (int j = i + 1; j < sp.length; j++)
                context.write(new Text(sp[i] + "-" + sp[j]), new Text(friend));
    }
}
