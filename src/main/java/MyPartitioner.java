import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner<K, V> extends org.apache.hadoop.mapreduce.Partitioner<K, V> {
    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        Text text = (Text)key;
        String line = text.toString();
        String name= line.split("#")[0];
        return Math.abs(name.hashCode() * 127) % numReduceTasks;

    }
}

