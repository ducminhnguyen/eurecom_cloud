package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;


public class ReduceSideJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputPath;
    private int numReducers;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "ReduceSideJoin-Duc-Ha");
        //job.setInputFormatClass(FileInputFormat.class);

        job.setMapperClass(ReduceSideJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(HashSet.class);

        job.setReducerClass(ReduceSideJoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setOutputFormatClass(FileOutputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setNumReduceTasks(numReducers);
        job.setJarByClass(ReduceSideJoin.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
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



}

class ReduceSideJoinMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    public ReduceSideJoinMapper(){
        super();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        DoubleWritable first = new DoubleWritable(Double.parseDouble(stringTokenizer.nextToken()));
        DoubleWritable second = new DoubleWritable(Double.parseDouble(stringTokenizer.nextToken()));
        context.write(new Text(first.toString()), second);
        context.write(new Text(second.toString()), first);
    }
}

class ReduceSideJoinReducer extends Reducer<Text, DoubleWritable, DoubleWritable, DoubleWritable> {

    @Override
    public void reduce(DoubleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        HashSet<DoubleWritable> result = new HashSet<>();
        for (DoubleWritable value : values) {
            result.add(value);
        }

        DoubleWritable[] temp = (DoubleWritable[]) result.toArray();
        for (int i = 0 ; i < temp.length - 1; ++i) {
            for (int j = i + 1; j< temp.length;++j) {
                context.write(temp[i], temp[j]);
            }
        }
    }
}