package com.ccb.training;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class StartWithCountJob extends Configured implements Tool {
	
  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "StartsWithCount");
	job.setJarByClass(getClass());
	// configure output and input source
	TextInputFormat.addInputPath(job, new Path(args[0]));
	job.setInputFormatClass(TextInputFormat.class);
	// configure mapper and reducer
	job.setMapperClass(StartWithCountMapper.class);
	job.setCombinerClass(StartWithCountReducer.class);
	job.setReducerClass(StartWithCountReducer.class);
	// configure output
	TextOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	return job.waitForCompletion(true) ? 0 : 1;
  }
	
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new StartWithCountJob(), args);
    System.exit(exitCode);
  }
}
