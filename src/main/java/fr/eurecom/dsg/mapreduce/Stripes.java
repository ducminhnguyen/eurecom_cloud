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
import java.util.HashMap;


public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        // define new job
        Job job = new Job(conf, "word-co-occurrences-stripes");
        // set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // set map class and the map output key and value classes
        job.setMapperClass(StripesMapper.class);
        job.setMapOutputValueClass(StringToIntMapWritable.class);
        job.setMapOutputKeyClass(Text.class);
        // set reduce class and the reduce output key and value classes
        job.setCombinerClass(StripesReducer.class);
        job.setReducerClass(StripesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringToIntMapWritable.class);
        // set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // add the input file as job input (from HDFS) to the variable inputPath
        FileInputFormat.addInputPath(job, inputPath);
        // set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, outputDir);
        // set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // set the jar class
        job.setJarByClass(Stripes.class);

        return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public Stripes(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }
}

class StripesMapper
        extends Mapper<LongWritable, // input key type
        Text, // input value type
        Text, // output key type
        StringToIntMapWritable> { // output value type

    @Override
    protected void map(LongWritable offset, //  input key type
                       Text line, // input value type
                       Context context) throws IOException, InterruptedException {

        String words[] = line.toString().split("\\s+");
        Matches matches = new Matches();

        for (int i = 0; i < words.length - 1; i++) {
            Text first = new Text(words[i]);
            for (int j = i + 1; j < words.length; j++) {
                if (!words[i].equals(words[j])) {
                    Text second = new Text(words[j]);
                    matches.addMatch(first, second);
                    matches.addMatch(second, first);
                }
            }
        }

        HashMap<Text, StringToIntMapWritable> map = matches.map;
        for (Text key : map.keySet()) {
            context.write(key, map.get(key));
        }

    }
}


class StripesReducer
        extends Reducer<Text,   // input key type
        StringToIntMapWritable,   // input value type
        Text,   // output key type
        StringToIntMapWritable> { // output value type

    @Override
    protected void reduce(Text word, // input key type
                          Iterable<StringToIntMapWritable> values, // input value type
                          Context context) throws IOException, InterruptedException {

        Matches matches = new Matches();

        for (StringToIntMapWritable wordMatches : values) {
            HashMap<Text, Long> map = wordMatches.map;
            for (Text key : map.keySet()) {
                matches.addMatch(word, key, map.get(key));
            }
        }

        HashMap<Text, StringToIntMapWritable> map = matches.map;
        for (Text key : map.keySet()) {
            context.write(key, map.get(key));
        }

    }
}