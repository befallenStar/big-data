import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InvertedIndexCombiner extends Reducer<Text,Text,Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(Text v:values)
        {
            count+=Integer.parseInt(v.toString());
        }
        String[] sp=key.toString().split("\t");
        context.write(new Text(sp[0]),new Text(sp[1]+":"+count));
    }
}
