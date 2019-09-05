import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<Weather, Text> {
    @Override
    public int getPartition(Weather weather, Text text, int i) {
        return (weather.getYear() - 1940) % i;
    }
}
