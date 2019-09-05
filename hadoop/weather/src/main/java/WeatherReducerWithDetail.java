import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducerWithDetail extends Reducer<Weather,Text,Weather, Text> {
    private static final int K=1;
    @Override
    protected void reduce(Weather key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        int sum=0;
        int average=0;
        for (Text v : values) {
            count++;
            if(count<=K)
            context.write(key, new Text("max"));
            sum+=key.getDegree();
        }
        average=sum/count;
        context.write(new Weather(key.getYear(),average),new Text("average"));
    }
}
