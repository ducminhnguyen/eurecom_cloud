package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Pair extends Configured implements Tool {


    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    public Pair(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Pair <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Pair(args), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        // define new job
        Job job = new Job(conf, "word-co-occurrences pair");
        // set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // set map class and the map output key and value classes
        job.setMapperClass(PairMapper.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(TextPair.class);
        // set reduce class and the reduce output key and value classes
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(LongWritable.class);
        // set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, inputPath);
        // set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, outputDir);
        // set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // set the jar class
        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job

    }

    public static class PairMapper
            extends Mapper<LongWritable, // input key type
            Text, // input value type
            TextPair, // output key type
            LongWritable> { // output value type

        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable offset, //  input key type
                           Text line, // input value type
                           Context context) throws IOException, InterruptedException {

            String words[] = line.toString().split("\\s+");
            // Map method
            for (int i = 0; i < words.length - 1; i++) {
                if (words[i].length() > 0) {
                    for (int j = i + 1; j < words.length; j++) {
                        if (!words[i].equals(words[j]) && words[j].length() > 0) {
                            context.write(new TextPair(words[i], words[j]), ONE);
                            context.write(new TextPair(words[j], words[i]), ONE);
                        }
                    }
                }
            }
        }
    }

    public static class PairReducer
            extends Reducer<TextPair, // input key type
            LongWritable, // input value type
            TextPair, // output key type
            LongWritable> { // output value type

        LongWritable sum  = new LongWritable();

        @Override
        protected void reduce(TextPair pair, // input key type
                              Iterable<LongWritable> values, // input value type
                              Context context) throws IOException, InterruptedException {

            long result = 0;

            for (LongWritable value : values) {
                result += value.get();
            }

            sum.set(result);
            context.write(pair, sum);
        }

    }
}
