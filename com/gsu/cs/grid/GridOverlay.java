package com.gsu.cs.grid;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gsu.cs.distributedcache.DistributedCacheMapper;
import com.gsu.cs.distributedcache.DistributedCacheOverlay;

public class GridOverlay  extends Configured implements Tool
{
	
	public int run(String[] arg) throws Exception 
	{
		JobConf conf = new JobConf( getConf(), GridOverlay.class);
		if (conf == null) 
		{
		 return -1;
	    }

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(GridMapper.class);
	conf.setReducerClass(GridReducer.class);
	
	conf.setInt("dimxy", Integer.parseInt(arg[0]));
	conf.setFloat("lowerX", Float.parseFloat(arg[1]));
	conf.setFloat("lowerY", Float.parseFloat(arg[2]));
	conf.setFloat("upperX", Float.parseFloat(arg[3]));
	conf.setFloat("upperY", Float.parseFloat(arg[4]));
	
	FileInputFormat.setInputPaths(conf, new Path("In"));
	FileOutputFormat.setOutputPath(conf,new Path("Out"));
	
	JobClient.runJob(conf);
	return 0;

	}
	
	public static void main(String[] args) throws Exception 
	{
	 int exitCode = ToolRunner.run(new GridOverlay(), args);
	 System.exit(exitCode);
	}
}
