import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Weather, Text, Weather, NullWritable> {
    private static final int K=1;
    @Override
    protected void reduce(Weather key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=1;
        for (Text v : values) {
            context.write(key, NullWritable.get());
            if(++count>K)
                break;
        }
    }
}
