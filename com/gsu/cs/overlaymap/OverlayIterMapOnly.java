package com.gsu.cs.overlaymap;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class OverlayIterMapOnly
extends Mapper<Object, Text, IntWritable, Text>
{
	//int basePolyCount = 7739;//4332;
	
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
	          //int polyKey = Integer.parseInt(polygonKey);
	          //context.write(new IntWritable(polyKey), new Text(buf.toString()));
	          context.write(null, new Text(buf.toString()));
	          //System.out.println("base  " + polyKey + "   " + buf);
	      }
    }
	
	/*private void cleanUp() 
	{
	}*/
}
	
	
