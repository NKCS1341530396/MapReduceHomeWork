import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
//hadoop jar /home/2021st19/WordCount.jar WordCount hdfs://master001:9000/data/wuxia_novels /user/2021st19/output7
public class Reduce extends Reducer<Text, LongWritable, Text, Text> {
    // 存储当前词语
    private Text word_name = new Text();
    //存储对应文件中当前词语出现的次数
    private Text word_book = new Text();
    //存储当前的key值
    private static Text CurrentItem = new Text(" ");
    //存储最终的结果
    private static List<String> postingList = new ArrayList<String>();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String name_list = key.toString();
        String book_name = name_list.split("#")[1];
        String name = name_list.split("#")[0];
        word_name.set(name);
        long sum = 0L;
        for(LongWritable value:values){
            sum += value.get();
        }
        //存储对应文件中当前词语出现的次数
        word_book.set(book_name+":"+sum);
        //如果当前词语发生了改变或者不为初始值，那么此时postingstr中存储了对应的结果，将其提取出来
        if (!CurrentItem.equals(word_name) && !CurrentItem.equals(" ")) {
            StringBuilder out = new StringBuilder();
            long count = 0;
            //开始对各个文件中当前词语出现的次数进行加和
            for (String p : postingList) {
                out.append(p);
                out.append(",");
                count += Long.parseLong(p.substring(p.indexOf(":") + 1));
            }
            //计算平均出现次数
            DecimalFormat df = new DecimalFormat("#.00");
            String avgFreq = df.format((double)count / (double)postingList.size());
            String final_result = avgFreq + "," + out.toString();
            if (count > 0) {
                //将最终结果输入到output文件中
                context.write(CurrentItem, new Text(final_result));
            }
            //postingstr重新清空
            postingList = new ArrayList<String>();
        }
        //记录当前的词语
        CurrentItem = new Text(word_name);
        //添加对应记录
        postingList.add(word_book.toString());
    }
}
