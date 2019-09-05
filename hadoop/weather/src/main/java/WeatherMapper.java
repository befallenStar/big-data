import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeatherMapper extends Mapper<LongWritable, Text, Weather, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sp = line.split("\t");
        String year = sp[0].substring(0, 4);
        String degree = sp[1].substring(0, sp[1].length() - 1);
        Weather weather = new Weather(Integer.parseInt(year), Integer.parseInt(degree));
        context.write(weather, value);
    }
}
