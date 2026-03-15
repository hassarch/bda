import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class MatrixMultiplication {
 
    // Mapper Class
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
 
            // Process matrix A
            if (indicesAndValue[0].equals("A")) {
                outputKey.set(indicesAndValue[2]); // Row index of A
                outputValue.set("A," + indicesAndValue[1] + "," + indicesAndValue[3]); // Col index of A, value
                context.write(outputKey, outputValue);
            }
            // Process matrix B
            else {
                outputKey.set(indicesAndValue[1]); // Column index of B
                outputValue.set("B," + indicesAndValue[2] + "," + indicesAndValue[3]); // Row index of B, value
                context.write(outputKey, outputValue);
            }
        }
    }
 
    // Reducer Class
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            ArrayList<Entry<Integer, Float>> listA = new ArrayList<Entry<Integer, Float>>();
            ArrayList<Entry<Integer, Float>> listB = new ArrayList<Entry<Integer, Float>>();
 
            // Separate matrix A and B values
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("A")) {
                    listA.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                } else {
                    listB.add(new SimpleEntry<Integer, Float>(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
                }
            }
 
            // Matrix multiplication logic
            Text outputValue = new Text();
            for (Entry<Integer, Float> a : listA) {
                String i = Integer.toString(a.getKey());
                float a_ij = a.getValue();
                for (Entry<Integer, Float> b : listB) {
                    String k = Integer.toString(b.getKey());
                    float b_jk = b.getValue();
                    outputValue.set(i + "," + k + "," + Float.toString(a_ij * b_jk));
                    context.write(key, outputValue); // Use key as the output key
                }
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 
        Job job = Job.getInstance(conf, "MatrixMatrixMultiplicationTwoSteps");
        job.setJarByClass(MatrixMultiplication.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Accept input and output paths as arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

start-all.sh

jps

mapred classpath

export

gedit 
javac


move class file to a folder

jar cf mm.jar *.class
jar tf mm.jar

gedit input.txt

hadoop fs -put input.txt /input
hadoop jar mm.jar matrixMultiplication /input /output
hadoop fs -ls /output