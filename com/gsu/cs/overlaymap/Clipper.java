package com.gsu.cs.overlaymap;

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

public class Clipper extends Configured implements Tool  
{

	@Override
	public int run(String[] args) throws IOException
	{
	JobConf conf = new JobConf( getConf(), Clipper.class);
	if (conf == null) {
	return -1;
	}
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(ClipMapper.class);
	//conf.setReducerClass(ClipReducer.class);
	conf.setNumMapTasks(3);
	//conf.set("mapred.tasktracker.map.tasks.maximum","1");
	//conf.setNumMapTasks(7);
    //conf.set("mapred.tasktracker.reduce.tasks.maximum","1");
	//System.out.println("Run " + args[0]);
	DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
	
	FileInputFormat.setInputPaths(conf, new Path("In"));
	 //FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileInputFormat.setInputPaths(conf, new Path("In"));
    //output folder needs to be deleted every time
    //FileOutputFormat.setOutputPath(conf,new Path("Out"));
	FileOutputFormat.setOutputPath(conf,new Path("Out"));
	JobClient.runJob(conf);
	return 0;
	}

	public static void main(String[] args) throws Exception 
	{
	int exitCode = ToolRunner.run(new Clipper(), args);
	System.exit(exitCode);
	}
}
	

