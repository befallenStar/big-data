package com.junenatte.hadoop.union.unitall;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UnionUnitAllReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text v : values)
            context.write(v, NullWritable.get());
    }
}
