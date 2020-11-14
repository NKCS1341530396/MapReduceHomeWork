import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
* 代码思路：先在map函数中将string切割成为单个词语，然后在词语后面附上所属的文件名
* 在Mypartition函数里边，根据词语进行reduce任务的分类
* 这时在Reduce函数中，得到的一连串key值是类似于：江湖#xxx1，江湖#xxx2，类似于这种，具体见Reduce函数
* */

public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.out.println(args[0].toString());
        String input_path = args[0];
        String output_path = args[1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"wordcount");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(Map.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(Reduce.class);

        //job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
