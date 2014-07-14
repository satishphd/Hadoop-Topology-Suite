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

import com.gsu.cs.overlaymap.ClipMapper;
import com.gsu.cs.overlaymap.Clipper;

public class GridOverlay  extends Configured implements Tool
{
    private static int m_dimxy;
	@Override
	public int run(String[] arg0) throws Exception 
	{
		JobConf conf = new JobConf( getConf(), GridOverlay.class);
		if (conf == null) 
		{
		 return -1;
	    }

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(Text.class);
	conf.setMapperClass(GridMapper.class);
	//conf.setReducerClass(GridReducer.class);
	conf.setNumReduceTasks(0);
	//conf.set("mapred.tasktracker.map.tasks.maximum","1");
	
	conf.setInt("dimxy", m_dimxy);
	
	FileInputFormat.setInputPaths(conf, new Path("In"));
	FileOutputFormat.setOutputPath(conf,new Path("Out"));
	
	JobClient.runJob(conf);
	return 0;

	}
	
	public static void main(String[] args) throws Exception 
	{
	int exitCode = ToolRunner.run(new GridOverlay(), args);
	 m_dimxy = Integer.parseInt(args[0]);
	System.exit(exitCode);
	}
}
