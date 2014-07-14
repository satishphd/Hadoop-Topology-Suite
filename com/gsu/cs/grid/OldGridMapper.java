package com.gsu.cs.grid;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.seisw.util.geom.PolyDefault;

/*
 * each mapper will find the appropriate tile for a given 
 * polygon and delegate it to that tile 
 */
public class OldGridMapper extends MapReduceBase
implements Mapper<LongWritable, Text, IntWritable, Text> 
{
	int TILE_COUNT = 4; 
     
    double minx = 34.4825652323176;
    double miny = -80.771518;
    double maxx = 36.561976;
    double maxy = -77.746617;
    
    // maxx - minx = 2.079410767682404, RANGE/4 = 0.519852691920601
    
    //double minx = -0.5;
    //double miny = 0.5;
    //double maxx = 6.0;
    //double maxy = 8; 
    
    Tile []tiles = {new Tile(34.4825652323176,-80.771518, 35.0024179242382, -77.746617), new Tile(35.0024179242382, -80.771518, 35.52227061615880, -77.746617),
    		new Tile(35.52227061615880, -80.771518,36.04212330807941 ,-77.746617),new Tile(36.04212330807941, -80.771518 , 36.561976, -77.746617)};
    //Tile []tiles = {new Tile(-0.5,0.5,1.5,8), new Tile(1.5,0.5,3,8), new Tile(3,0.5,4.5,8), new Tile(4.5,0.5,6,8)};
   
	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector<IntWritable, Text> collector, Reporter arg3)
			throws IOException 
	{
		  //break a string into tokens	
	      PolyDefault p1 = null;
	      String strId = null;
	      int id;
	      //String dummy = new String();
	      
	      if(value.toString().charAt(0) == 'b') //base polygon
	      { 
	      
	          StringTokenizer itr = new StringTokenizer(value.toString());
	         
	          while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
	          {	  
	        	  String tag = itr.nextToken(); // temp change  for literal b
	              strId = itr.nextToken(); 
	              id = Integer.parseInt(strId);
	              p1 = new PolyDefault();
	              
	              Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
	              p1.setM_lbBox(lbox);
	           
	              Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
	              p1.setM_ubBox(ubox);
	              //dummy = dummy + tag + String.valueOf(id);
	              
	              for(int i = 0; i< tiles.length; i++)
	              {
	            	// if the bounding line starts in the tile  
	            	if(lbox.getX() >= tiles[i].low.getX() && lbox.getX() <= tiles[i].high.getX())
	            	{
	            		//definitely in tile i
	            		collector.collect(new IntWritable(i), value);
	            		continue;
	            	}
	            	
	            	// if the bounding line ends in the tile
	            	if(ubox.getX() >= tiles[i].low.getX() && ubox.getX() <= tiles[i].high.getX())
	            	{
	            		collector.collect(new IntWritable(i), value);
	            		continue;
	            	}
	            	// if the line is passing through the tile with none of its end points in the tile
	            	if(lbox.getX() <= tiles[i].low.getX() && ubox.getX() >= tiles[i].high.getX())
	            	{
	            		//definitely in tile i
	            		collector.collect(new IntWritable(i), value);
	            	}
	              }
	            
	              break;
	          } //end while
	      }	//end if
	      else if(value.toString().charAt(0) != 'b') //base polygon
	      {
	    	  StringTokenizer itr = new StringTokenizer(value.toString());
		         
	          while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
	          {	  
	              strId = itr.nextToken(); 
	              id = Integer.parseInt(strId);
	              p1 = new PolyDefault();
	              
	              Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
	              p1.setM_lbBox(lbox);
	           
	              Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
	              p1.setM_ubBox(ubox);
	              //dummy = dummy + String.valueOf(id);
	              
	              for(int i = 0; i< tiles.length; i++)
	              {
	            	// if the bounding line starts in the tile  
	            	if(lbox.getX() >= tiles[i].low.getX() && lbox.getX() <= tiles[i].high.getX())
	            	{
	            		//definitely in tile i
	            		collector.collect(new IntWritable(i), value);
	            		continue;
	            	}
	            	
	            	// if the bounding line ends in the tile
	            	if(ubox.getX() >= tiles[i].low.getX() && ubox.getX() <= tiles[i].high.getX())
	            	{
	            		collector.collect(new IntWritable(i), value);
	            		continue;
	            	}
	            	// if the line is passing through the tile with none of its end points in the tile
	            	if(lbox.getX() <= tiles[i].low.getX() && ubox.getX() >= tiles[i].high.getX())
	            	{
	            		//definitely in tile i
	            		collector.collect(new IntWritable(i), value);
	            	}
	              }
	            
	              break;
	          } //end while 
	      }
		
    } //end Map task

}
/*
 700 mb file parcelXML.xml
 <MINX>-80.4356285902495</MINX>, <MINY>34.9085185474876</MINY>
<MAXX>-78.3949667784168</MAXX>, <MAXY>36.3146799992723</MAXY>

small file 16mb polygonXML.xml = 
<MINX>-80.771518</MINX> ,<MINY>34.4825652323176</MINY>
<MAXX>-77.746617</MAXX> ,<MAXY>36.561976</MAXY>

for 16 * 700 dataset
grid dimension = <MINX>-80.771518</MINX> and <MINY>34.4825652323176</MINY>
<MAXX>-77.746617</MAXX> and <MAXY>36.561976</MAXY> 
*/