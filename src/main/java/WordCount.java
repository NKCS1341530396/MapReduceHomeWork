import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;
//整体思路是将小说名字加入到输入文件中，然后Map的时候，key值变成 小说名字:单词 这样的形式，在得到reduce结果后，对 小说名字:名字:次数 进行拆分，
//每一个单词在result里对应类似于result[i]，在result[i]里对应最终结果，最后输出的时候，只需要把总次数除以对应文档数即可
public class WordCount {
    //对应HDFS的url，使用时把中间ip换成虚拟机里边对应的ip
    static String path = "hdfs://192.168.238.130:9000";
    //输入文件url，不过在使用jar的时候，还是要输入文件URL的，我不确定有没有冲突
    static String input_path = path + "/user/hadoop/inputdata";
    //输出文件URL
    static String output_path = path + "/user/hadoop/output";
    //存储最终的结果，0,1,2,3...对应小说名字，最后一位对应总次数
    static public List<List<Double>> results = new ArrayList<>();
    //存储对应单词在results列表中的位置
    static public HashMap<String, Integer> words = new HashMap<>();
    //小说名字跟results里对应的位置
    static public HashMap<String, Integer> books = new HashMap<>();

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        String[] Path = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapreduce.framework.name", "yarn");
        //conf.set("yarn.resourcemanager.hostname", "localhost");
        conf.set("fs.defaultFS", path);
        Job job = Job.getInstance(conf);
        upload_file();
        getFileUpload(input_path, conf);
        System.out.println(getFileUpload(input_path, conf).toString());


        FileInputFormat.setInputPaths(job, new Path(input_path));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        job.setPartitionerClass(HashPartitioner.class);
        //job.setNumReduceTasks(1);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(output_path));
        //job.waitForCompletion(true);
        job.submit();
        get_output(conf);
        System.out.println(results.toString());
    }
    //将本地文件上传到HDFS文件系统中
    static void upload_file() throws IOException {
        String data_path = "E:\\课程\\MapReduce\\第三次作业\\exp2_sample_data";
        File file = new File(data_path);
        File[] data_list = file.listFiles();
        for (int i = 0; i < data_list.length; i++) {
            String file_name = data_list[i].getName();
            String upload_txt = input_path + "/" + file_name.split("\\.")[0];
            books.put(file_name.split("\\.")[0], i);
            write_file(file_name.split("\\.")[0], data_list[i].getPath());
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
    //在数据文件末尾添加对应的小说名字
    static void write_file(String filename, String filepath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, true));
        writer.append(' ');
        writer.append(filename);
        writer.close();
    }
    //获取HDFS文件系统中对应位置的文件名，可以用于判断文件是否上传成功
    static List<String> getFileUpload(String file_path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<String> results = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(new Path(file_path));
        for (int i = 0; i < fileStatus.length; i++) {
            FileStatus fileStatu = fileStatus[i];
            String oneFilePath = fileStatu.getPath().getName();
            results.add(oneFilePath);
        }
        fs.close();
        return results;
    }
    //将MapReduce结果拆分到results里边
    static void get_output(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(output_path));
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder sb = new StringBuilder();
        String line = "";
        while (( line =br.readLine())!= null){
            insert_value(line);
        }
        double count = 0;
        //对每条记录除以对应文档数
        for(int i=0;i<results.size();i++){
            count = 0;
            for(int j=0;j<results.get(i).size() - 1;j++){
                if(results.get(i).get(j)!=0)count += 1;
            }
            results.get(i).set(results.get(i).size() - 1,
                    results.get(i).get(results.get(i).size() - 1) / count);
        }
    }
    //针对单挑记录的拆分
    static void insert_value(String line){
        String[] list = line.split(":");
        String name = list[1];
        int count = Integer.parseInt(list[2]);
        if(WordCount.words.containsKey(name)){
            int pos = WordCount.words.get(list[0]);
            double value = WordCount.results.get(pos).get(WordCount.books.size() - 1) +  count;
            WordCount.results.get(pos).set(WordCount.books.size() - 1, value);
            WordCount.results.get(pos).set(WordCount.books.get(list[0]), (double) count);
        }else {
            List a = new ArrayList<Integer>();
            int pos = WordCount.books.get(list[0]);
            for(int i = 0;i <WordCount.books.size();i++ ){
                a.add(i == pos?(int)count:0);
            }
            a.add((int)count);
            WordCount.results.add(a);
            WordCount.words.put(list[1], WordCount.results.size() - 1);
        }
    }

}