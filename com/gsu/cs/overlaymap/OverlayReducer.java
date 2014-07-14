package com.gsu.cs.overlaymap;

import com.gsu.cs.sequential.Parser;
import com.gsu.cs.sequential.PolyLowXComparator;
import com.gsu.cs.sequential.RelationshipGraph;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class OverlayReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> 
{
	
	public void reduce(IntWritable key, Iterable<Text> values, 
               Context context) throws IOException, InterruptedException 
    {
	  int keyValue;	
	  List<PolyDefault> overlayPolyList = new ArrayList<PolyDefault>();
	  List<PolyDefault> basePolyList = new ArrayList<PolyDefault>();	
      int count = 0;
     
      PolyDefault basePolygon = null;
      
      for(Text value : values)
      {
          PolyDefault p1 = new PolyDefault();
          StringTokenizer itr = new StringTokenizer(value.toString());
          String strId = null;
          int id = 0;
          
          if(value.toString().charAt(0) == 'b')
          {  	  
        	  while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
              {	  
                 if(count == 0)
           	     {
                   itr.nextToken(); //disregard the b
                   strId = itr.nextToken(); 
                   id = Integer.parseInt(strId); 
                   
                   p1.setId(id);
                   Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
                   p1.setM_lbBox(lbox);
             
                   Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
                   p1.setM_ubBox(ubox);
           	     }
           	     count = count + 1;
              } 
        	  basePolygon = p1;
          }
          else
          {
        	  while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
              {	  
                if(count == 0)
           	    {
                  strId = itr.nextToken(); 
                  id = Integer.parseInt(strId); 
                  p1.setId(id);
                  
                  Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
                  p1.setM_lbBox(lbox);
             
                  Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
                  p1.setM_ubBox(ubox);
           	    }
           	     count = count + 1;
              }
        	  overlayPolyList.add(p1);
          }
          count = 0;
      }
      
      if(basePolygon != null)
      {
        // sort the overlay Polygons 
        Collections.sort(overlayPolyList,new PolyLowXComparator());
	    System.out.println("Reduce " + basePolygon.getId() + " " + overlayPolyList.size());
	    basePolyList.add(basePolygon);	
        RelationshipGraph.createRelationships(basePolyList, overlayPolyList);
        overlayPolyList = basePolyList.get(0).getOverlayPoly();
        StringBuffer buf = new StringBuffer();
       
        Iterator<PolyDefault> itr = overlayPolyList.iterator();
        PolyDefault dummy;
        while(itr.hasNext())
        {
    	   dummy = itr.next();
    	   buf.append(dummy.getId() + " ");
        }
        context.write(new IntWritable(basePolygon.getId()), new Text(buf.toString()));
      } //end if
      else
      {
    	  context.write(key,null);
      }
     } // end reduce   
  }