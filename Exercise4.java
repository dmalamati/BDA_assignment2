import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class Exercise4 {

    // Mapper Class to process degree data
    public static class DegreeMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
        private final IntWritable node = new IntWritable();
        private final FloatWritable probability = new FloatWritable();
        private float threshold;

        // The map function processes each line of input data
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tokenizing the input string
            StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            this.threshold = conf.getFloat("threshold", 0); // Retrieve the threshold from the configuration
            while (itr.hasMoreTokens()) {
                // Parsing the input tokens
                int node1 = Integer.parseInt(itr.nextToken());
                int node2 = Integer.parseInt(itr.nextToken());
                float prob = Float.parseFloat(itr.nextToken());

                // If the probability is greater than the threshold, emit the node and probability pair
                if(prob > this.threshold){
                    node.set(node1);
                    probability.set(prob);
                    context.write(node, probability);  // Emit node1 with the probability

                    node.set(node2);
                    context.write(node, probability);  // Emit node2 with the probability
                }
            }
        }
    }

    // Mapper Class for calculating average probability
    public static class AvgMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
        private final IntWritable avg = new IntWritable();
        private final FloatWritable degree = new FloatWritable();

        // The map function processes each line of input data
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                int node1 = Integer.parseInt(itr.nextToken());
                float prob = Float.parseFloat(itr.nextToken());
                avg.set(0); // Set the output key to a fixed value
                degree.set(prob); // Set the degree value
                context.write(avg, degree);  // Emit the key 0 with the degree
            }
        }
    }

    // Reducer Class for summing up probabilities by node
    public static class DegreeReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        private final FloatWritable result = new FloatWritable();
        private final Map<IntWritable, Float> nodeSums = new HashMap<>();

        // The reduce function sums the values (probabilities) for each node
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get(); // Sum all the probabilities for the node
            }
            result.set(sum);  // Set the sum as the result
            nodeSums.put(key, result.get());  // Store the result in the map
            context.write(key, result);  // Emit the node and its summed probability
        }
    }

    // Reducer Class to calculate the average probability
    public static class AvgReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        private final FloatWritable result = new FloatWritable();
        private final Map<IntWritable, Float> nodeSums = new HashMap<>();
        float average = 0;
        int n = 0;

        // The reduce function calculates the average of the degrees
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            for (FloatWritable val : values) {
                average += val.get();  // Sum up the degrees
                n += 1;  // Count the number of values
            }
            average = average / n;  // Calculate the average
            result.set(average);  // Set the average as the result
            nodeSums.put(key, result.get());  // Store the result in the map
            context.write(key, result);  // Emit the average degree
        }
    }

    // Reducer Class for filtering nodes based on summed probability
    public static class DegreeFilterReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
        private final FloatWritable result = new FloatWritable();
        private final Map<IntWritable, Float> nodeSums = new HashMap<>();
        float average = 0;

        // The reduce function filters out nodes with a summed probability below the average threshold
        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            Configuration conf = context.getConfiguration();
            this.average = conf.getFloat("average.threshold", 0);  // Get the average threshold from configuration

            for (FloatWritable val : values) {
                sum += val.get();  // Sum the probabilities
            }
            if(sum >= average){  // Only emit nodes with a summed probability above the threshold
                result.set(sum);
                nodeSums.put(key, result.get());  // Store the result
                context.write(key, result);  // Emit the node and its filtered summed probability
            }
        }
    }

    // Main Method that runs the MapReduce jobs
    public static void main(String[] args) throws Exception {

        // Set up the first job (Degree calculation)
        Configuration conf = new Configuration();
        float threshold = Float.parseFloat(args[0]);  // Parse the threshold value from command-line arguments
        conf.setFloat("threshold", threshold);  // Set the threshold in the configuration
        Job job = Job.getInstance(conf, "degrees");
        job.setJarByClass(Exercise4.class);
        job.setMapperClass(DegreeMapper.class);
        job.setCombinerClass(DegreeReducer.class);
        job.setReducerClass(DegreeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/collins.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output4_degrees"));

        // Wait for job completion and check for errors
        if (!job.waitForCompletion(true)) {
            System.err.println("Job failed!");
            System.exit(1);
        }

        // Set up the second job (Average calculation)
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "mean degree");
        job1.setJarByClass(Exercise4.class);
        job1.setMapperClass(AvgMapper.class);
        job1.setReducerClass(AvgReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job1, new Path("output4_degrees/part-r-*"));  // Use wildcard to include all part-r-* files
        FileOutputFormat.setOutputPath(job1, new Path("output4_mean_degree"));

        // Wait for job completion and check for errors
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job failed!");
            System.exit(1);
        }

        // Read the result of the mean degree job
        FileSystem fs = FileSystem.get(conf1);
        Path resultFilePath = new Path("output4_mean_degree", "part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(resultFilePath)));
        String line = br.readLine();
        String[] keyValue = line.split("\t");  // Split the line into key-value pair
        float value = Float.parseFloat(keyValue[1]);  // Parse the average value
        br.close();
        fs.close();

        // Set up the third job (Filtering based on average threshold)
        Configuration conf2 = new Configuration();
        conf2.setFloat("average.threshold", value);  // Pass the average threshold to the next job
        Job job2 = Job.getInstance(conf2, "filtered degrees");
        job2.setJarByClass(Exercise4.class);
        job2.setMapperClass(DegreeMapper.class);
        job2.setCombinerClass(DegreeFilterReducer.class);
        job2.setReducerClass(DegreeFilterReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job2, new Path("input/collins.txt"));
        FileOutputFormat.setOutputPath(job2, new Path("output4_filtered_degrees"));

        // Wait for job completion and check for errors
        System.exit((job2.waitForCompletion(true) && job1.waitForCompletion(true) && job.waitForCompletion(true)) ? 0 : 1);
    }
}
