package com.junenatte.hadoop.weblogs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PVMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        WebLog wl = WebLog.filterData(line);
        if (wl.getRequest() != null)
            if (wl.isValid())
                context.write(new Text(wl.getRequest()), new IntWritable(1));
    }
}
