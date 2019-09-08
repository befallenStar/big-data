import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class InvertedIndexTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path dst = new Path("/main/invertedindex_output/");
            if (hdfs.exists(dst))
                hdfs.delete(dst, true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(InvertedIndexTest.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, "/main/sjh/books/*.txt");
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, dst);

            job.setMapperClass(InvertedIndexMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(InvertedIndexCombiner.class);

            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            boolean b = job.waitForCompletion(true);
        } catch (InterruptedException | IOException |
                ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
