package com.ccb.training;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;

public class StartWithCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable countOne = new IntWritable(1);
  private final Text reusableText = new Text();
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

	  StringTokenizer tokenizer = new StringTokenizer(value.toString());
	  while (tokenizer.hasMoreTokens()) {
	    reusableText.set(tokenizer.nextToken().substring(0, 1));
	    context.write(reusableText, countOne);
	  }
  }
}
