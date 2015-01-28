package com.gsu.cs.chained;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OverlayPhase1
{
	 public static void main(String[] args) throws Exception 
	 {
		    // for job configuraton
			Configuration conf = new Configuration();
		    //for job control  
		    Job job = new Job (conf, "overlay");
		    
		    job.setJarByClass(OverlayPhase1.class);
		    
		    job.setMapperClass(OverlayPhase1Map.class);
		    job.setNumReduceTasks(0);
		    
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path("In"));
		    //output folder needs to be deleted every time
		    FileOutputFormat.setOutputPath(job,new Path("Out"));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
}
