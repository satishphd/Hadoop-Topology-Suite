package com.gsu.cs.distributedcache;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DistributedCacheOverlay extends Configured implements Tool  
{

	public int run(String[] args) throws IOException
	{
	JobConf conf = new JobConf( getConf(), DistributedCacheOverlay.class);
	if (conf == null) {
	return -1;
	}
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(DistributedCacheMapper.class);
	
	//conf.setNumMapTasks(3);
	
	DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
	
	FileInputFormat.setInputPaths(conf, new Path("In"));

	FileOutputFormat.setOutputPath(conf,new Path("Out"));
	JobClient.runJob(conf);
	return 0;
	}

	public static void main(String[] args) throws Exception 
	{
	int exitCode = ToolRunner.run(new DistributedCacheOverlay(), args);
	System.exit(exitCode);
	}
}
	

