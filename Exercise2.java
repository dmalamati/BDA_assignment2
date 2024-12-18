import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Exercise2 {

    // Custom function for reading the movies.csv and splitting correctly the values of each column.
    public static String[] parseCSVLine(String line) {
        // If it's the first line of the csv file it skips it.
        if (line.startsWith("imdbID,title,year,runtime,genre,released,imdbRating,imdbVotes,country")) {
            return new String[0];
        }

        // Using regex it parses correctly nested quotes with commas inside, and splits the fields into an array.
        String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        List<String> result = new ArrayList<>();
        // For each field
        for (String field : fields) {
            // Trims all the white characters and removes the double quotes ("") if they exist within the field.
            String cleanedField = field.trim().replaceAll("^\"|\"$", "");
            // The field is added in the result only if it's not empty.
            if (!cleanedField.isEmpty()) {
                result.add(cleanedField);
            }
        }

        return result.toArray(new String[0]);
    }

    // DurationPerCountryMapper: writes for each movie (input line) the country and the duration as a pair.
    public static class DurationPerCountryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text country = new Text();
        private IntWritable duration = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = parseCSVLine(value.toString());
            // If a line doesn't have the correct amount of columns it skips it.
            if (fields.length != 9){
                return;
            }
            try {
                // Finds the movie's runtime (4th column - 3rd starting from 0).
                String runtimeStr = fields[3].trim();
                int runtime;
                // Removes the string "min" if it exists and then turns the runtime into an integer.
                if (runtimeStr.endsWith("min")) {
                    runtime = Integer.parseInt(runtimeStr.replace(" min", ""));
                } else{
                    runtime = Integer.parseInt(runtimeStr);
                }

                // Create a pair (country, duration) for each country listed in the movie's country field (9th column).
                String[] countries = fields[8].split(",");
                for (String c : countries) {
                    country.set(c.trim());
                    duration.set(runtime);
                    context.write(country, duration);
                }
            } catch (NumberFormatException e) {
                // Skip lines with wrong number or content of fields
            }
        }
    }

    // DurationPerCountryReducer: based on the name of the country(key) it sums all the durations(values) of every
    // occurrence and writes the new pairs where each country appears only once.
    public static class DurationPerCountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable totalDuration = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalDuration.set(sum);
            context.write(key, totalDuration);
        }
    }

    // MoviesPerYearAndGenreMapper: writes for each movie (input line) a pair that contains as the key its release year
    // and genre as "year_genre" that have imdbRating>8, and as value 1.
    public static class MoviesPerYearAndGenreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text year_genre = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = parseCSVLine(value.toString());
            // If a line doesn't have the correct number of columns it skips it.
            if (fields.length != 9) return;
            try {
                // Finds the year of the movie (3rd column)
                String year = fields[2].trim();
                // Finds the movie's imdbrating (7th column), trims the white characters and converts it into a double.
                double imdbRating = Double.parseDouble(fields[6].trim());

                // If a movie has an imdbRating over 8
                if (imdbRating > 8) {
                    // It finds the movie's genres (5th column) and creates a pair (year_genre, 1) for each genre listed in the movie's genre field.
                    String[] genres = fields[4].split(",");
                    for (String genre : genres) {
                        year_genre.set(year + "_" + genre.trim());
                        context.write(year_genre, one);
                    }
                }
            } catch (NumberFormatException e) {
                // Skip lines with wrong number or content of fields
            }
        }
    }

    // MoviesPerYearAndGenreReducer: based on the release year and genre(key) it sums all the occurrences.
    public static class MoviesPerYearAndGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job1 calculates the total duration of movies per country
        Job job1 = Job.getInstance(conf, "duration per country");
        job1.setJarByClass(Exercise2.class);
        job1.setMapperClass(DurationPerCountryMapper.class);
        job1.setReducerClass(DurationPerCountryReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("input/movies.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("output2_1"));
        job1.waitForCompletion(true);

        // Job2 calculates the number of movies with imdbRating>8 per year and genre
        Job job2 = Job.getInstance(conf, "movies per year and genre");
        job2.setJarByClass(Exercise2.class);
        job2.setMapperClass(MoviesPerYearAndGenreMapper.class);
        job2.setReducerClass(MoviesPerYearAndGenreReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("input/movies.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("output2_2"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
