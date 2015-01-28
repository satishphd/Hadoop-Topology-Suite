package com.gsu.cs.chained;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class OverlayPhase1Map
extends Mapper<Object, Text, IntWritable, Text>
{	
	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException 
    {
		  //break a string into tokens	
	      StringBuffer buf = null;
	      String polygonKey = null;
	      
	      if(value.toString().charAt(0) == 'b') //base polygon
	      { 
	      
	          StringTokenizer itr = new StringTokenizer(value.toString());
	         
	          while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
	          {	  
	        	      buf = new StringBuffer();
	        	      
	        	      buf.append(itr.nextToken() + " ");
	        	      
	        	      polygonKey = itr.nextToken();
	        	      
	        	      buf.append(polygonKey + " ");
	        		  //lowerbox
	                  buf.append(itr.nextToken() + " ");
	                  buf.append(itr.nextToken() + " "); 
	                  //upperbox
	                  buf.append(itr.nextToken() + " ");
	                  buf.append(itr.nextToken());
	                  break;
	          }
	         
	          context.write(null, new Text(buf.toString()));
	      }
    }
}
	
	
