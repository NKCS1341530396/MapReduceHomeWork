import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0L;
        for(LongWritable value:values){
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}
