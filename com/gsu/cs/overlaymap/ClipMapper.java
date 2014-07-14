package com.gsu.cs.overlaymap;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.gsu.cs.sequential.Parser;
import com.gsu.cs.sequential.PolyLowXComparator;
import com.gsu.cs.sequential.RelationshipGraph;
import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class ClipMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> 
{
    List<PolyDefault> basePolyList = new ArrayList<PolyDefault>();
   	int outputPolyCount = 0;
	@Override
	public void configure(JobConf conf) 
	{
	
	 try 
	 {
	  System.out.println("cacheRelationship start");
	  Path[] patternsFiles;
	  patternsFiles = DistributedCache.getLocalCacheFiles(conf);
      parse(patternsFiles[0]);
	 }
	 catch (IOException e) 
	 {
	  System.out.println("I/O exception");
	 }
    }
	
	private void parse(Path path)
	{
		try 
		{
		 System.out.println(path.toString());
         BufferedReader br = new BufferedReader(new FileReader(path.toString()));
		 String strLine;
		 PolyDefault p1;
		 int id;
		 int count;
		 PolyDefault basePolygon = null;
		 //Read File Line By Line
		 while ((strLine = br.readLine()) != null) 	
		 {
		     //System.out.println(strLine);
			 count = 0;
			 p1 = new PolyDefault();
	         StringTokenizer itr = new StringTokenizer(strLine.toString());
	         String strId = null;
	         id = 0;
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
          	     double xCord,yCord;
               
                 xCord = Double.parseDouble(itr.nextToken());
                 yCord = Double.parseDouble(itr.nextToken());
                 Point2D vertex = new Point2D.Double(xCord,yCord);
             
                 p1.add(vertex);
             } 
	         //System.out.println(p1.getM_lbBox().getX());
       	     basePolyList.add(p1);
       	     
		 }
		 Collections.sort(basePolyList,new PolyLowXComparator());
		}
		catch(IOException e)
		{
			System.out.println("dude learn some exception handling");
		}
	}
	
	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<Text, IntWritable> collector, Reporter arg3)
			throws IOException
	{
	    //dont read base layer file
		int count = 0;
		String strId = null;
		PolyDefault p1 = null;
		int id;
		
		if(value.toString().charAt(0) != 'b')
	    {
			StringTokenizer itr = new StringTokenizer(value.toString());
			p1 = new PolyDefault();
			while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
            {	  
              if(count == 0)
         	  {
            	  //itr.nextToken(); // temp change  
                strId = itr.nextToken(); 
                id = Integer.parseInt(strId); 
                p1.setId(id);
                
                Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
                p1.setM_lbBox(lbox);
           
                Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
                p1.setM_ubBox(ubox);
         	  }
         	   count = count + 1;
         	    
         	   double xCord,yCord;
               
               xCord = Double.parseDouble(itr.nextToken());
               yCord = Double.parseDouble(itr.nextToken());
               Point2D vertex = new Point2D.Double(xCord,yCord);
           
               p1.add(vertex);
            }//end while
			
			  double upperBaseX = 0.0;
	          int range;
	          PolyDefault result;
	          String resultStr;
	          upperBaseX = p1.getM_ubBox().getX(); //clip poly
	          /* Find the polygon with index where base's upperBox is no less than overlay's lower box */
	          range = RelationshipGraph.BinarySearch(basePolyList, upperBaseX);
	          //List<PolyDefault> remaining = overlayPolygons.GetRange(0, range);
	          List<PolyDefault> remaining = basePolyList.subList(0, range);
	          
	          for(PolyDefault p: remaining)
	          {
	            if (RelationshipGraph.isOverlap(p1, p))
	            {
	                	  result = (PolyDefault)Clip.intersection(p,p1);
	                	  if(result.isEmpty() == false)
	            		  {
	                	   outputPolyCount = outputPolyCount + 1;	  
	            		   resultStr = Parser.serializePoly(result).toString();
	            		   collector.collect(new Text(resultStr), new IntWritable(p.getId()));
	            		  }
	            }
	          }
	      }//end if
		  System.out.println("Number of output polygons is " + outputPolyCount);
		  	
	}
}
