package com.gsu.cs.overlaymap;


import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.seisw.util.geom.PolyDefault;

public class OverlayMapper 
     extends Mapper<Object, Text, IntWritable, Text>
{
  int basePolyCount = 4332;  
  
  //key is offset and value is line of text
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException 
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
          int polyKey = Integer.parseInt(polygonKey);
          context.write(new IntWritable(polyKey), new Text(buf.toString()));
          //System.out.println("base  " + polyKey + "   " + buf);
      }
      else
      {
    	  StringTokenizer itr = new StringTokenizer(value.toString());
          
          while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
          {	  
        	      buf = new StringBuffer();
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
    	  
          for(int i = 0; i < basePolyCount; i++)
    	  { 
             context.write(new IntWritable(i+1), new Text(buf.toString()));
             //System.out.println("clip  " + (i+1) + "   " + buf);
    	  }
    
      }
  }
}
      