import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeatherMapReduce {

    // Mapper Class
    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Parse CSV line
            String[] fields = value.toString().split(",");

            String city = fields[0];
            String temperature = fields[1];

            String weatherCondition = getWeatherCondition(temperature);

            context.write(new Text(city), new Text(weatherCondition));
        }

        private String getWeatherCondition(String temperature) {

            double temp = Double.parseDouble(temperature);

            if (temp < 0) {
                return "Freezing";
            } else if (temp >= 0 && temp <= 15) {
                return "Cold";
            } else if (temp > 15 && temp <= 25) {
                return "Warm";
            } else {
                return "Hot";
            }
        }
    }

    // Reducer Class
    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder conditions = new StringBuilder();

            for (Text val : values) {
                conditions.append(val.toString()).append(" ");
            }

            context.write(key, new Text(conditions.toString().trim()));
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Weather MapReduce");

        job.setJarByClass(WeatherMapReduce.class);

        job.setMapperClass(WeatherMapper.class);
        job.setCombinerClass(WeatherReducer.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}