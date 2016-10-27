package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 *
 */
public class WordCount extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;




    @Override
    public int run(String[] args) throws Exception  {

        Configuration conf = this.getConf();
        Job job = null; // TODO: define new job instead of null using conf

        // TODO: set job input format
        // TODO: set map class and the map output key and value classes
        // TODO: set reduce class and the reduce output key and value classes
        // TODO: set job output format
        // TODO: add the input file as job input (from HDFS) to the variable
        //       inputPath
        // TODO: set the output path for the job results (to HDFS) to the variable
        //       outputPath
        // TODO: set the number of reducers using variable numberReducers
        // TODO: set the jar class
        job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCReducer.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCount (String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCount <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception  {
        int res = ToolRunner.run(new Configuration(), new WordCount(args), args);
        System.exit(res);
    }
}

class WCMapper extends Mapper<Object, // TODO: change Object to input key type
        Text, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, // TODO: change Object to input key type
                       Text value, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // TODO: implement the map method (use context.write to emit results)
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }

}

class WCReducer extends Reducer<Text, // TODO: change Object to input key type
        IntWritable, // TODO: change Object to input value type
        Text, // TODO: change Object to output key type
        IntWritable> { // TODO: change Object to output value type

    private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, // TODO: change Object to input key type
                          Iterable<IntWritable> values, // TODO: change Object to input value type
                          Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method (use context.write to emit results)
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);

    }
}