import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class Map extends  Mapper<LongWritable, Text, Text, LongWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split_data = value.toString().split(" ");
        String Book_name = split_data[split_data.length - 1];
        for(int i =0;i < split_data.length - 1;i++) {
            String lineword = Book_name+ ":" + split_data[i];
            System.out.println(lineword);
            context.write(new Text(lineword), new LongWritable(1));
        }
    }
}
