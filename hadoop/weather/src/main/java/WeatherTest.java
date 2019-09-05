import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WeatherTest {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs= FileSystem.get(conf);
            Path dst=new Path("/main/weather_output/");
            if(hdfs.exists(dst))
                hdfs.delete(dst,true);
            Job job=Job.getInstance(conf);
            job.setJarByClass(WeatherTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,"/main/sjh/weather.txt");

            job.setMapperClass(WeatherMapper.class);
            job.setMapOutputKeyClass(Weather.class);
            job.setMapOutputValueClass(Text.class);

            job.setPartitionerClass(WeatherPartitioner.class);
            job.setNumReduceTasks(3);

            job.setGroupingComparatorClass(WeatherGroup.class);
            job.setSortComparatorClass(WeatherSortByDegreeDESC.class);

            job.setReducerClass(WeatherReducerWithDetail.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Weather.class);
            job.setOutputValueClass(Text.class);
            TextOutputFormat.setOutputPath(job,dst);
            boolean b=job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
