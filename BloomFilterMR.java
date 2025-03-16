import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import java.io.IOException;

public class BloomFilterMR extends Mapper<Object, Text, Text, BloomFilterWritable> {
    private BloomFilterWritable bloomFilterWritable = new BloomFilterWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim(); // Trim to remove extra spaces

        if (line.isEmpty()) {
            return; // Skip empty lines to avoid IllegalArgumentException
        }

        // Initialize a BloomFilter
        BloomFilter bloomFilter = new BloomFilter(1024, 5, org.apache.hadoop.util.hash.Hash.MURMUR_HASH);

        // Convert text input to a BloomFilter key
        Key bloomKey = new Key(line.getBytes());
        bloomFilter.add(bloomKey);

        // Wrap BloomFilter in BloomFilterWritable
        bloomFilterWritable.setBloomFilter(bloomFilter);
        context.write(new Text("bloom"), bloomFilterWritable);
    }
}

