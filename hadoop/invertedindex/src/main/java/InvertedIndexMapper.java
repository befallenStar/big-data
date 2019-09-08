import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable,Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName= ((FileSplit)context.getInputSplit()).getPath().getName();
        String line=value.toString();
        StringTokenizer stringTokenizer=new StringTokenizer(line);
        while(stringTokenizer.hasMoreTokens()){
            String word=stringTokenizer.nextToken();
            context.write(new Text(word+'\t'+fileName),new Text("1"));
        }
    }
}
