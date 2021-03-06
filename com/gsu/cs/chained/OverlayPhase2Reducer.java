package com.gsu.cs.chained;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.gsu.cs.sequential.Parser;
import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class OverlayPhase2Reducer extends MapReduceBase
implements Reducer<IntWritable,Text, IntWritable, Text> 
{
	IntWritable outputKey = new IntWritable();
  
    HashMap<Text,List<Text>> map = new HashMap<Text,List<Text>>();
    
	public void reduce(IntWritable key, Iterator<Text> values,
			OutputCollector<IntWritable,Text> collector, Reporter arg3)
			throws IOException 
    {
		List<PolyDefault> overlayPolyList = null;
		
	    Text clipText; 
		
	    int count = 0;
		String strcount = " ";
		PolyDefault basePolygon = null;
		
		int keyValue;	
		
		//values contain a basepolygon and/or one or more clip polygons
		//so value is one polygon
		PolyDefault clipPoly;
		overlayPolyList = new ArrayList<PolyDefault>();
		
		 while(values.hasNext())
		 {
			Text text = values.next(); 
			Text temp = new Text();
			temp.set(text);
			count = 0;
			
			if(temp.charAt(0) == 'b')
			{
				basePolygon = new PolyDefault();
				StringTokenizer baseItr = new StringTokenizer(temp.toString());
	           
	            while (baseItr.hasMoreTokens()) // tokens are strings from one serialized polygon
	            {     
	              if(count == 0)
	               {
	               baseItr.nextToken(); //discard
	               baseItr.nextToken(); //index or line number
	               Point2D lbox = new Point2D.Double(Double.parseDouble(baseItr.nextToken()), Double.parseDouble(baseItr.nextToken()));
	               basePolygon.setM_lbBox(lbox);
	      
	               Point2D ubox = new Point2D.Double(Double.parseDouble(baseItr.nextToken()),Double.parseDouble(baseItr.nextToken()));
	               basePolygon.setM_ubBox(ubox);
	               }
	               count = count + 1;
	        
	              double xCord,yCord;
	      
	              xCord = Double.parseDouble(baseItr.nextToken());
	              yCord = Double.parseDouble(baseItr.nextToken());
	              Point2D vertex = new Point2D.Double(xCord,yCord);
	        
	              basePolygon.add(vertex);
	             }
			}
			else
			{
				  
				  StringTokenizer clipItr = new StringTokenizer(temp.toString());
	              clipPoly = new PolyDefault();
	              
	              while(clipItr.hasMoreTokens())
	              {
	                if(count == 0)
	                {
	                 clipItr.nextToken(); //discard the index or line number
	                 Point2D lbox = new Point2D.Double(Double.parseDouble(clipItr.nextToken()), Double.parseDouble(clipItr.nextToken()));
	                 clipPoly.setM_lbBox(lbox);
	           
	                 Point2D ubox = new Point2D.Double(Double.parseDouble(clipItr.nextToken()),Double.parseDouble(clipItr.nextToken()));
	                 clipPoly.setM_ubBox(ubox);
	                }
	                count = count + 1;
	             
	                double xCord,yCord;
	           
	                xCord = Double.parseDouble(clipItr.nextToken());
	                yCord = Double.parseDouble(clipItr.nextToken());
	                Point2D vertex = new Point2D.Double(xCord,yCord);
	             
	                clipPoly.add(vertex);
	              }
	                overlayPolyList.add(clipPoly);
			}
		  }
		
			
		if(overlayPolyList.size() > 0)
		{
			  PolyDefault result; 
		        
		      for(int j =0; j<overlayPolyList.size(); j++)
		      {
		      		  result = (PolyDefault)Clip.intersection(basePolygon, 
		      				 overlayPolyList.get(j));
		      		  if(result.isEmpty() == false)
		      		  {
		      		   keyValue = key.get();
		      		   outputKey.set(keyValue);
		      		   collector.collect(outputKey, Parser.serializePoly(result));
		      		  }
		       }
		}
	   }
 }		