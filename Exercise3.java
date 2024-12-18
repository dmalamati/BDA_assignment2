import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exercise3 {

    // SubSequenceMapper: For each line, it parses the line 3 times to find all the sub-sequences with length 2, 3 and 4,
    // and then for each sub-sequence wrights a pair that contains as the key the sub-sequence and as value 1.
    public static class SubSequenceMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text sequence = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int[] lengths = {2, 3, 4};

            // For each length size (2,3,4)
            for (int n : lengths) {
                // Find all existing sequential sub-strings with length n
                for (int i = 0; i <= line.length() - n; i++) {
                    String sub_sequence = line.substring(i, i + n);
                    sequence.set(sub_sequence);
                    context.write(sequence, one);
                }
            }
        }
    }

    // IntSumReducer: it sums all the occurrences of each sub-sequence the mapper found.
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "sequence count");
        job.setJarByClass(Exercise3.class);
        job.setMapperClass(SubSequenceMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/ecoli.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
