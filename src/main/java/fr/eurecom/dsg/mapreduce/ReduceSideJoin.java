package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.net.SyslogAppender;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReduceSideJoin-Duc-Ha");
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(ReduceSideJoin.ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(HashSet.class);

        job.setReducerClass(ReduceSideJoin.ReduceSideJoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(numReducers);
        job.setJarByClass(ReduceSideJoin.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return job.waitForCompletion(true) ? 0 : 1; // TODO: implement all the job components andconfigurations
    }

    public ReduceSideJoin(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: ReduceSideJoin <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }

        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(args), args);
        System.exit(res);
    }


    public static class ReduceSideJoinMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public ReduceSideJoinMapper() {

        }
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            IntWritable first = new IntWritable(Integer.parseInt(stringTokenizer.nextToken()));
            IntWritable second = new IntWritable(Integer.parseInt(stringTokenizer.nextToken()));
            context.write(first, second);
            context.write(second, first);
        }
    }

    public static class ReduceSideJoinReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public ReduceSideJoinReducer() {

        }
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            HashSet<IntWritable> result = new HashSet<>();
            for (IntWritable value : values) {
                result.add(value);
            }

            IntWritable[] temp = (IntWritable[]) result.toArray();
            for (int i = 0 ; i < temp.length - 1; ++i) {
                for (int j = i + 1; j< temp.length;++j) {
                    context.write(temp[i], temp[j]);
                }
            }
        }
    }
}