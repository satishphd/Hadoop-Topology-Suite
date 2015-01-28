package com.gsu.cs.grid;

import gnu.trove.TIntProcedure;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.rtree.RTree;

public class GridMapper extends MapReduceBase
implements Mapper<LongWritable, Text, IntWritable, Text>, TIntProcedure
{
	RTree tree = new RTree();
	List<Integer> cellList = new ArrayList<Integer>();

	public void configure(JobConf conf) 
	{
		float lowerX = 1.0f;
		float lowerY = 1.0f;
		float upperX = 1.0f;
		float upperY = 1.0f;

		int rowCount = 1;

		conf.getInt("dimxy", rowCount);
		conf.getFloat("lowerX", lowerX);
		conf.getFloat("lowerY", lowerY);
		conf.getFloat("upperX", upperX);
		conf.getFloat("upperY", upperY);

		//perfect square grid with row = column
		int columnCount = rowCount;

		//initialize the RTree
		tree.init(null);

		double [][]grid = new double[rowCount][columnCount];
		GridPartition.partition(rowCount*columnCount, lowerX, lowerY, upperX, upperY, grid);

		for(int i = 0; i<rowCount;i++)
		{
			Rectangle rect = new Rectangle();
			rect.minX = (float)grid[i][0];
			rect.minY = (float)grid[i][1];
			rect.maxX = (float)grid[i][2];
			rect.maxY = (float)grid[i][3];
			tree.add(rect, i);
		}
	}

	public void map(LongWritable arg0, Text value, OutputCollector<IntWritable, Text> collector,
			Reporter arg3) throws IOException 
    {
		//break a string into tokens	
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

				Rectangle rect = new Rectangle();
				rect.minX = Float.parseFloat(itr.nextToken());
				rect.minY = Float.parseFloat(itr.nextToken());
				rect.maxX = Float.parseFloat(itr.nextToken());;
				rect.maxY = Float.parseFloat(itr.nextToken());

				tree.intersects(rect, this);

				Iterator<Integer> cellItr = cellList.iterator();
				while(cellItr.hasNext())
				{
					int cellId = cellItr.next();
					collector.collect(new IntWritable(cellId), value);
				}
				break;
			} //end while
			System.out.println("base size " + cellList.size());
			cellList.clear();
		}	//end if
		else if(value.toString().charAt(0) != 'b') //base polygon
		{
			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) // tokens are strings from one serialized polygon 
			{	  
				strId = itr.nextToken(); 
				id = Integer.parseInt(strId);
				Rectangle rect = new Rectangle();
				rect.minX = Float.parseFloat(itr.nextToken());
				rect.minY = Float.parseFloat(itr.nextToken());
				rect.maxX = Float.parseFloat(itr.nextToken());;
				rect.maxY = Float.parseFloat(itr.nextToken());

				tree.intersects(rect, this);   

				Iterator<Integer> cellItr = cellList.iterator();
				while(cellItr.hasNext())
				{
					int cellId = cellItr.next();
					collector.collect(new IntWritable(cellId), value);
				} 

				break;
			} //end while
			cellList.clear();
		}
   }


	public boolean execute(int cellId)
	{
		cellList.add(cellId);
		return true;
	}
}
