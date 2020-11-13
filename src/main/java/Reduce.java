import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, LongWritable, Text, Text> {
    private Text word_name = new Text();
    private Text word_book = new Text();
    private static Text CurrentItem = new Text(" ");
    private static List<String> postingList = new ArrayList<String>();
    private static long[] count_list = new long[WordCount.books.size() + 1];
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        String name_list = key.toString();
        String book_name = name_list.split("#")[1];
        String name = name_list.split("#")[0];
        word_name.set(name);
        long sum = 0L;
        for(LongWritable value:values){
            sum += 1;
        }
        word_book.set(book_name+":"+sum);
        if (!CurrentItem.equals(word_name) && !CurrentItem.equals(" ")) {
            StringBuilder out = new StringBuilder();
            long count = 0;
            for (String p : postingList) {
                out.append(p);
                out.append(",");
                count += Long.parseLong(p.substring(p.indexOf(":") + 1));
            }
            //计算平均出现次数
            DecimalFormat df = new DecimalFormat("#.00");
            String avgFreq = df.format((double)count / (double)postingList.size());
            String final_result = word_name.toString() + " "+avgFreq + "," + out.toString();
            if (count > 0) {
                Text result = new Text();
                result.set(final_result);
                System.out.println(final_result);
                context.write(result, new Text(""));
            }
            postingList = new ArrayList<String>();
        }
        CurrentItem = new Text(word_name);
        postingList.add(word_book.toString());
        //outputCollector.collect(text, new IntWritable(sum));
    }
}
