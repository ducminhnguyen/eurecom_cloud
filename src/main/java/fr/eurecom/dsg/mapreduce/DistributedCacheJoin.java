package fr.eurecom.dsg.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.MRJobConfig;

public class DistributedCacheJoin extends Configured implements Tool {

    private Path outputDir;
    private Path inputFile;
    private Path inputTinyFile;
    private int numReducers;

    public DistributedCacheJoin(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: DistributedCacheJoin <num_reducers> " +
                    "<input_tiny_file> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputTinyFile = new Path(args[2]);
        this.inputFile = new Path(args[1]);
        this.outputDir = new Path(args[3]);
        //System.console().printf(inputTinyFile.toUri().getRawPath());
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // TODO: add the smallFile to the distributed cache

        Job job = Job.getInstance(conf); // TODO: define new job instead of null using conf e setting
        // a name

        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // TODO: set map class and the map output key and value classes
        job.setMapperClass(DCJMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(DCJReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // TODO: add the input file as job input (from HDFS) to the variable
        // inputFile
        FileInputFormat.addInputPath(job, inputFile);
        // TODO: set the output path for the job results (to HDFS) to the variable
        FileOutputFormat.setOutputPath(job, outputDir);

        // outputPath
        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(numReducers);
        // TODO: set the jar class
        job.setJarByClass(DistributedCacheJoin.class);
        System.console().printf(inputTinyFile.getFileSystem(getConf()).getUri().getRawPath());

        DistributedCache.addCacheFile(inputTinyFile.getFileSystem(getConf()).getUri(), getConf());
        //job.addCacheFile(inputTinyFile.toUri());


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new DistributedCacheJoin(args),
                args);
        System.exit(res);
    }
}


// TODO: implement mapper
class DCJMapper extends Mapper<Object, Text, Text, IntWritable>  {
    private Configuration _conf;
    private BufferedReader _reader;
    private HashSet<String> _wordsToSkip = new HashSet<String>();
    private Text _word = new Text();
    private final IntWritable _one = new IntWritable(1);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        _conf = context.getConfiguration();
        URI[] uris = DistributedCache.getCacheFiles(_conf);
        System.console().printf(uris[0].getRawPath());
        for(URI uri : uris){
            Path path = new Path(uri.getPath());
            String fileName = path.getName();
            parseSkipFile(fileName);
        }
    }

    private void parseSkipFile(String fileName) {
        try{
            _reader = new BufferedReader(new FileReader(fileName));
            String word = null;
            while((word = _reader.readLine()) != null){
                _wordsToSkip.add(word);
            }
        }
        catch (IOException io){
            System.err.print(fileName + " is not found!");
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        String w;
        while (stringTokenizer.hasMoreTokens()) {
            w = stringTokenizer.nextToken();
            if(!_wordsToSkip.contains(w)){
                _word.set(w);
                context.write(_word, _one);
            }
        }
    }
}

// TODO: implement reducer
class DCJReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable _count = new IntWritable(0);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int count = 0;
        for(IntWritable i :values){
            count += i.get();
        }
        _count.set(count);
        context.write(key, _count);
    }
}