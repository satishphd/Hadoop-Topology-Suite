package com.gsu.cs.Iter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class finalMapper extends Mapper<Object, Text, Text, IntWritable>
{
	protected void setup(Mapper.Context context)
  throws IOException,InterruptedException
         {
		   System.out.println("setup");
         }

	public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException 
   {
		System.out.println("map");
   }
	
	protected void cleanup(Mapper.Context context)
    throws IOException,InterruptedException
    {
		System.out.println("cleanup");
    }
}
