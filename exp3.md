import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MovieTags {

    // Mapper Class
    public static class TagMapper extends Mapper<Object, Text, Text, Text> {

        private Text movieId = new Text();
        private Text tag = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Format: userId,movieId,tag,timestamp
            String[] tokens = value.toString().split(",");

            if (tokens.length == 4) {
                movieId.set(tokens[1]);
                tag.set(tokens[2]);
                context.write(movieId, tag);
            }
        }
    }

    // Reducer Class
    public static class TagReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Set<String> tags = new HashSet<>();

            // Collect unique tags for each movie
            for (Text val : values) {
                tags.add(val.toString());
            }

            // Join tags into a single string
            result.set(String.join(", ", tags));

            context.write(key, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: MovieTags <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Movie Tags");

        job.setJarByClass(MovieTags.class);

        job.setMapperClass(TagMapper.class);
        job.setReducerClass(TagReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

Input:
1,101,comedy,1234567890
2,101,drama,1234567891
3,102,action,1234567892
4,101,comedy,1234567893
5,102,thriller,1234567894

Steps to be executed:
Type in Terminal:

1) hadoop namenode -format
2) start-all.sh (To start hadoop localhost)

Browser:
1) Type:
Localhost:9870
Type in Terminal:
3) jps
4) mapred classpath
5)export CLASSPATH = “&lt;copy the output of step 4&gt;”
6) gedit MovieTags.java (Write the java code in this)
7) javac MovieTags.java
Open Ubuntu File System
Move all other class files except the class file named MovieTags into another folder.
Only class files of MovieTags should remain in main directory
Type in Terminal
8) jar cf MT.jar *.class
9) jar tf MT.jar (for viewing the jar file)
10) gedit input3.csv (Write properly input content in this file)
11) hadoop fs -put input3.csv /input3