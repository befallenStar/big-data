import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private static IntWritable num=new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable v:values){
            context.write(num,key);
            num.set(num.get()+1);
        }
    }
}
