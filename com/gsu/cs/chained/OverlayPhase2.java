package com.gsu.cs.chained;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gsu.cs.chained.OverlayPhase2Mapper;

public class OverlayPhase2  extends Configured implements Tool
{
	public int run(String[] args) throws IOException
	{
	JobConf conf = new JobConf( getConf(), OverlayPhase2.class);
	if (conf == null) {
	return -1;
	}
	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(OverlayPhase2Mapper.class);
	
	conf.setReducerClass(OverlayPhase2Reducer.class);
	conf.setNumMapTasks(2);
	conf.setNumReduceTasks(8);
	//conf.set("mapred.tasktracker.map.tasks.maximum","1");
	
	DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
	//DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);
	
	FileInputFormat.setInputPaths(conf, new Path("Input"));
	
	FileOutputFormat.setOutputPath(conf,new Path("Out"));
	JobClient.runJob(conf);
	return 0;
	}

	public static void main(String[] args) throws Exception 
	{
	int exitCode = ToolRunner.run(new OverlayPhase2(), args);
	System.exit(exitCode);
	}
}
