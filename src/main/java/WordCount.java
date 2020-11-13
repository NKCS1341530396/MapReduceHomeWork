import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class WordCount {
    //对应HDFS的url，使用时把中间ip换成虚拟机里边对应的ip
    static String path = "hdfs://192.168.238.131:9000";
    //输入文件url，不过在使用jar的时候，还是要输入文件URL的，我不确定有没有冲突
    static String input_path = path + "/user/hadoop/input/*";
    static String file_path = path + "/user/hadoop/input";
    //输出文件URL
    static String output_path = path + "/user/hadoop/output18";
    //小说名字跟results里对应的位置
    static public List<String> books = new ArrayList<>() ;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"wordcount");
        //upload_file();
        System.out.println(getFileUpload(file_path, conf));

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
    //获取HDFS文件系统中对应位置的文件名，可以用于获取文件名
    static List<String> getFileUpload(String file_path, Configuration conf) throws IOException {
        Path path = new Path(file_path);
        FileSystem fs = path.getFileSystem(conf);
        List<String> results = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(new Path(file_path));
        for (int i = 0; i < fileStatus.length; i++) {
            FileStatus fileStatu = fileStatus[i];
            String oneFilePath = fileStatu.getPath().getName();
            books.add(oneFilePath);
            results.add(oneFilePath);
        }
        return results;
    }
    //将本地文件上传到HDFS文件系统中
    static void upload_file() throws IOException {
        String data_path = "E:\\课程\\MapReduce\\第三次作业\\exp2_sample_data";
        File file = new File(data_path);
        File[] data_list = file.listFiles();
        for (int i = 0; i < data_list.length; i++) {
            String file_name = data_list[i].getName();
            String upload_txt = file_path + "/" + file_name.split("\\.")[0];
            InputStream in = new BufferedInputStream(new FileInputStream(data_path + "\\" +
                    file_name));
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(upload_txt), conf);
            OutputStream out = fs.create(new Path(upload_txt), new Progressable() {
                public void progress() {
                    System.out.print(".");
                }
            });
            IOUtils.copyBytes(in, out, 4096, true);
            fs.close();
            in.close();
            out.close();
            System.out.println("success");
        }
    }

}
