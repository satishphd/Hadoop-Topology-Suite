package com.gsu.cs.grid;

import gnu.trove.TIntProcedure;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.gsu.cs.sequential.Parser;
import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.rtree.RTree;
import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class GridReducer extends MapReduceBase
implements Reducer<IntWritable,Text, IntWritable, Text>, TIntProcedure
{
	OutputCollector< IntWritable, Text> m_Collector = null;
	RTree tree = new RTree();
	private int m_BasePolyId;
	private HashMap<Integer,PolyDefault> m_BasePolyMap = new HashMap<Integer,PolyDefault>();
	private HashMap<Integer,PolyDefault> m_ClipPolyMap = new HashMap<Integer,PolyDefault>();
	
	public void configure(JobConf conf) 
	{
	  //initialize the RTree
	  tree.init(null);
	}
	
    // the key is grid id and values are the polygon texts
	@Override
	public void reduce(IntWritable gridId, Iterator<Text> values,
			OutputCollector<IntWritable, Text> collector, Reporter arg3)
			throws IOException 
    {
		m_Collector = collector;
		
		List<PolyDefault>basePolyList = new ArrayList<PolyDefault>();
        List<PolyDefault> overlayPolyList = new ArrayList<PolyDefault>();
		
	    Text clipText; 
		
	    int count = 0;
		String strcount = " ";
		PolyDefault basePolygon = null;
		
		String strBaseId;
		String strClipId;
		int baseId = 0;
		int clipId = 0;
		//values contain both basepolygons and clip polygons
		PolyDefault clipPoly;
		
		while(values.hasNext())
		{
			Text text = values.next(); 
			
			if(text.charAt(0) == 'b')
			{
				basePolygon = new PolyDefault();
				StringTokenizer baseItr = new StringTokenizer(text.toString());
	            //System.err.println("base : = " + baseText);
	            while (baseItr.hasMoreTokens()) // tokens are strings from one serialized polygon
	            {     
	              if(count == 0)
	               {
	                baseItr.nextToken(); //discard
	                strBaseId = baseItr.nextToken(); //index or line number 
	                baseId = Integer.parseInt(strBaseId);
	                
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
	             basePolyList.add(basePolygon);
	             m_BasePolyMap.put(baseId,basePolygon);
			}
			else
			{
				  
				  StringTokenizer clipItr = new StringTokenizer(text.toString());
	              clipPoly = new PolyDefault();
	              
	              while(clipItr.hasMoreTokens())
	              {
	                if(count == 0)
	                {
	                 //discard the index or line number
	                 strClipId =  clipItr.nextToken();  //index or line number
		             clipId = Integer.parseInt(strClipId);
		             
	                 Point2D lbox = new Point2D.Double(Double.parseDouble(clipItr.nextToken()), Double.parseDouble(clipItr.nextToken()));
	                 clipPoly.setM_lbBox(lbox);
	           
	                 Point2D ubox = new Point2D.Double(Double.parseDouble(clipItr.nextToken()),Double.parseDouble(clipItr.nextToken()));
	                 clipPoly.setM_ubBox(ubox);
	                 
	                 Rectangle r2 = new Rectangle((float)lbox.getX(), (float)lbox.getY(), (float)ubox.getX(), (float)ubox.getY());
					 tree.add(r2, clipId);
	                }
	                count = count + 1;
	             
	                double xCord,yCord;
	           
	                xCord = Double.parseDouble(clipItr.nextToken());
	                yCord = Double.parseDouble(clipItr.nextToken());
	                Point2D vertex = new Point2D.Double(xCord,yCord);
	             
	                clipPoly.add(vertex);
	              }
	              //overlayPolyList.add(clipPoly);
	              m_ClipPolyMap.put(clipId, clipPoly);
			}
		  } //end while
		  
		  Iterator<PolyDefault> itr = basePolyList.iterator();
		  Rectangle baseBox = new Rectangle();
		  PolyDefault basePoly;
		  
		  while(itr.hasNext())
		  {
			basePoly = itr.next();
		    baseBox.maxX = (float)basePoly.getM_ubBox().getX();
		    baseBox.maxY = (float)basePoly.getM_ubBox().getY();
		    baseBox.minX = (float)basePoly.getM_lbBox().getX();
		    baseBox.minY = (float)basePoly.getM_lbBox().getY();
			this.m_BasePolyId = basePoly.getId();
			
			tree.intersects(baseBox, this);
		  }
		
		 } //end reduce

	@Override
	public boolean execute(int clipPolyId) 
	{
		PolyDefault basePoly = m_BasePolyMap.get(m_BasePolyId);
		PolyDefault clipPoly = m_ClipPolyMap.get(clipPolyId);
		PolyDefault resultPoly = (PolyDefault)Clip.intersection(basePoly, clipPoly);
		
		String strResult;
		if(resultPoly.isEmpty() == false)
		{
		   strResult = Parser.serializePoly(resultPoly).toString();	
    	   try 
    	   {
			m_Collector.collect(null,new Text(strResult) );
			//System.out.println(strResult);
		   } 
    	   catch (IOException e) 
		   {
			// TODO Auto-generated catch block
			e.printStackTrace();
		   }
    	   
	    }
		return true;
	}
		
}


