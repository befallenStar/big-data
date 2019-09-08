import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text v : values) {
            sb.append(v.toString()).append(";");
        }
        context.write(key, new Text(sb.toString().substring(0,sb.toString().length()-1)));
    }
}
