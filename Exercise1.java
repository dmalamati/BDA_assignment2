import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exercise1 {

    // Mapper class for generating numeronyms from words in the input
    public static class NumeronymsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1); // Constant value for counting occurrences
        private Text word = new Text(); // Variable to store the numeronym

        // Map method processes each line of input and generates key-value pairs
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()); // Tokenizes input line into words
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken(); // Get the next token
                str = str.replaceAll("[^a-zA-Z]", ""); // Remove non-alphabetic characters
                str = str.toLowerCase(); // Convert word to lowercase
                if (str.length() > 2) { // Only process words with more than 2 characters
                    // Generate the numeronym
                    String n_str = str.charAt(0) + Integer.toString(str.length() - 2) + str.charAt(str.length() - 1);
                    word.set(n_str); // Set the numeronym as the key
                    context.write(word, one); // Write the key-value pair to context
                }
            }
        }
    }

    // Reducer class for aggregating the counts of each numeronym
    public static class NumeronymsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable(); // Variable to store the aggregated count
        private int k; // Threshold for filtering results

        // Reduce method processes all values for a given key
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.k = conf.getInt("threshold", 0); // Retrieve the threshold from the configuration
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get(); // Aggregate the count of occurrences
            }
            if (sum > this.k) { // Only write the output if the count exceeds the threshold
                result.set(sum);
                context.write(key, result); // Write the key and aggregated count to context
            }
        }
    }

    // Main method to configure and run the MapReduce job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[0]); // Parse the threshold value from command-line arguments
        conf.setInt("threshold", k); // Set the threshold in the configuration

        Job job = Job.getInstance(conf, "numeronyms"); // Create a new job instance
        job.setJarByClass(Exercise1.class); // Set the main class for the job
        job.setMapperClass(Exercise1.NumeronymsMapper.class); // Set the Mapper class
        job.setCombinerClass(NumeronymsReducer.class); // Set the Combiner class (optional)
        job.setReducerClass(NumeronymsReducer.class); // Set the Reducer class
        job.setOutputKeyClass(Text.class); // Specify the output key type
        job.setOutputValueClass(IntWritable.class); // Specify the output value type

        // Set the input and output paths for the job
        FileInputFormat.addInputPath(job, new Path("input/SherlockHolmes.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output1"));

        // Exit with appropriate status based on job success or failure
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

