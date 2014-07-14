package com.gsu.cs.overlaymap;

import gnu.trove.TIntProcedure;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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
import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.rtree.RTree;
import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class A extends MapReduceBase
implements Mapper<LongWritable, Text, IntWritable,Text>, TIntProcedure
{
	IntWritable baseKey = new IntWritable();
	OutputCollector< IntWritable, Text> collector = null;
	HashMap<Integer,PolyDefault> basePolyMap = new HashMap<Integer,PolyDefault>();
	
	//for storing polygons for reduce phase
	HashMap<Integer,Text> clipPolyValueMap = new HashMap<Integer,Text>();
	List<PolyDefault> clipPolyList = new ArrayList<PolyDefault>();
	RTree tree = new RTree();  
	private int clipPolyId;
	//static List<Integer> basePolyIdList = new ArrayList<Integer>();
	//clip to list of related base polys
	HashMap<Integer,List<Integer>> mapping =  new HashMap<Integer,List<Integer>>();
	boolean flag = false;
	public void configure(JobConf conf) 
	{
	 //filename = conf.get("map.input.file");
	 try 
	 {
	  //initialize the RTree
	  tree.init(null);
	  Path[] patternsFiles;
	  patternsFiles = DistributedCache.getLocalCacheFiles(conf);
	   
	  for (int i = 0; i<patternsFiles.length; i++)
	  {
       parse(patternsFiles[i]);
	  }
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
		
		 //PolyDefault basePolygon = null;
		 //Read File Line By Line
		 while ((strLine = br.readLine()) != null) 	
		 {
		     //System.out.println(strLine);
			 p1 = new PolyDefault();
	         StringTokenizer itr = new StringTokenizer(strLine.toString());
	         String strId = null;
	         id = 0;
	         while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
             {	  
                  itr.nextToken(); //disregard the b
                  strId = itr.nextToken(); 
                  id = Integer.parseInt(strId); 
                  
                  p1.setId(id);
                  Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()), Double.parseDouble(itr.nextToken())); 
                  p1.setM_lbBox(lbox);
            
                  Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
                  p1.setM_ubBox(ubox);
                  
                  Rectangle r2 = new Rectangle((float)lbox.getX(), (float)lbox.getY(), (float)ubox.getX(), (float)ubox.getY());
			      tree.add(r2, id);
             }//end inner while
		 }//end while
		}
		catch(IOException ex)
		{
			System.out.println(ex.getMessage());
		}
	}
                
	
	//key is offset and value is line of text
	@Override
	public void map(LongWritable arg0, Text value,
			OutputCollector< IntWritable, Text> collect, Reporter arg3)
			throws IOException
	{
		//dont read base layer file
		Text text;
		text = new Text();
		text.set(value);
		
		int count = 0;
		String strId = null;
		PolyDefault p1 = null;
		int id;
		//System.out.println("filename : " + filename);	
		if(collect != null)
		{
		 collector = collect;
		 //System.out.println("not null collector");
		}
	      
		if(text.toString().charAt(0) != 'b' )
	    {	
			
			flag = true;
			StringTokenizer itr = new StringTokenizer(text.toString());
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
                
                List<Integer> basePolyList = new ArrayList<Integer>();
    			mapping.put(id,basePolyList);
    			clipPolyValueMap.put(id, text);
    			//collector.collect(new IntWritable(id), clipText);
         	  }
         	   count = count + 1;
         	    
         	   double xCord,yCord;
               
               xCord = Double.parseDouble(itr.nextToken());
               yCord = Double.parseDouble(itr.nextToken());
               Point2D vertex = new Point2D.Double(xCord,yCord);
           
               p1.add(vertex);
            } //end while
			
			clipPolyList.add(p1);
	    } //end if
		else if(text.toString().charAt(0) == 'b' )
		{
			flag = true;
			//for base layer polygons
			StringTokenizer itr = new StringTokenizer(text.toString());
			p1 = new PolyDefault();
			while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
            {	  
              if(count == 0)
         	  {
            	itr.nextToken(); // temp change  for literal b
                strId = itr.nextToken(); 
                id = Integer.parseInt(strId); 
                p1.setId(id);
                //to keep track of local base polygons in a map task
                basePolyMap.put(id, p1); 
                baseKey.set(id);
                //collector.collect(new IntWritable(id), text);
                collector.collect(baseKey, text);
                
                //collector.collect(new IntWritable(id), new Text("base"));
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
            } //end while
			
			if(collect!=null)
			{
			 collector = collect;
			}
		}	
	}

	/*  callback function of Rtree's intersect method 
	 *  prepare local mapping between clip polygon and base  polygon list */
	@Override
	public boolean execute(int basePolyId)
	{	
		if(mapping.containsKey(clipPolyId))
		{
			List<Integer> list = mapping.get(clipPolyId);
			list.add(basePolyId);
		}
		return true;
	}
	
	public void close() throws IOException
	{
		// to make sure all base polygons and clip polygons in this filesplit are read
		// so that I can process them locally
		
		if(flag == true && collector != null)
		{
         //collector.collect(new Text("close begin "), new IntWritable(basePolyIdList.size()));
		
		int baseId;
		PolyDefault clipPoly,basePoly,resultPoly;
		clipPoly = basePoly = resultPoly = null;
		Rectangle clipBox = new Rectangle();
		String resultStr = null;
		
		Iterator<PolyDefault> itr = clipPolyList.iterator();
		
		while(itr.hasNext())
		{
			clipPoly = itr.next();
			clipBox.maxX = (float)clipPoly.getM_ubBox().getX();
			clipBox.maxY = (float)clipPoly.getM_ubBox().getY();
			clipBox.minX = (float)clipPoly.getM_lbBox().getX();
			clipBox.minY = (float)clipPoly.getM_lbBox().getY();
			this.clipPolyId = clipPoly.getId();
			
			tree.intersects(clipBox, this);
		}
		//if(collector!=null)
	    //collector.collect(new Text("close rtree "), new IntWritable(basePolyIdList.size()));
		
		// Create file 
		
//		
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(URI.create("hdfs://localhost/user/spuri2/local/mapout.txt"), conf);
//		OutputStream out = fs.create(new Path("hdfs://localhost/user/spuri2/local/mapoutput.txt"));
		
		  //FileWriter fstream = new FileWriter(filename);
		  //BufferedWriter out = new BufferedWriter(fstream);
		// compute local overlay for a given input split
		Iterator<PolyDefault> itr1 = clipPolyList.iterator();
		while(itr1.hasNext())
		{
			clipPoly = itr1.next();
				//this list contains base polys that are not locally available
			    List<Integer> baseIdList = mapping.get(clipPoly.getId());
			
			    Iterator<Integer> baseItr = baseIdList.iterator();
				while(baseItr.hasNext())
				{
					//System.out.println("Inside while base list size " + basePolyIdList.size());
					//if(basePolyMap.size() > 0)
           		     //collector.collect(new Text("while base "), new IntWritable(basePolyMap.size()));
					baseId =(int)baseItr.next();
					// for local processing, basePolyMap contains only local base polygons
					if(basePolyMap.containsKey(baseId))
					{
						basePoly = basePolyMap.get(baseId);
						//collector.collect(new Text("xx"), new IntWritable(basePoly.getId()));
						resultPoly = (PolyDefault)Clip.intersection(basePoly, clipPoly);
	                	if(resultPoly.isEmpty() == false)
	            		{
	                	   resultStr = Parser.serializePoly(resultPoly).toString();	
	                	   //collector.collect(null,new Text(resultStr) );
	                	   System.out.println(resultStr);
	           	        }
					}
					else
					{
						/* in reduce phase, ignore the value list whose size is 1 */
						Text clipPoly1 = clipPolyValueMap.get(clipPoly.getId());
						/* I know this base and clip intersect, if not local then in reduce phase */
						
						collector.collect(new IntWritable(baseId), clipPoly1);
						
					}
			     }
		  }
	}
  }
}
