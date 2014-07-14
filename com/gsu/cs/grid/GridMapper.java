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
		float lowerX = 1;
    	float lowerY = 0.5f;
    	float upperX = 10;
    	float upperY = 8;
		
    	int rowCount = 4;
    	
    	conf.getInt("dimxy", rowCount);
    	//perfect square grid with row = column
    	int columnCount = rowCount;
    	

    	//initialize the RTree
  	    tree.init(null);
    	
  	    double [][]grid = new double[rowCount][columnCount];
  	    partition(rowCount*columnCount,lowerX, lowerY, upperX, upperY, grid);
        for(int i = 0; i<rowCount;i++)
        {
        	Rectangle rect = new Rectangle();
        	rect.minX = (float)grid[i][0];
        	rect.minY = (float)grid[i][1];
        	rect.maxX = (float)grid[i][2];
        	rect.maxY = (float)grid[i][3];
        	tree.add(rect, i);
        	System.out.println("id "+ i + "-> minX " + rect.minX + " minY " + rect.minY +
        	 " maxX " + rect.maxX + " maxY " + rect.maxY);
        }
	}
	
	public void map(LongWritable arg0, Text value,
			OutputCollector<IntWritable, Text> collector, Reporter arg3)
			throws IOException 
	
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
	          System.out.println("clip size" + cellList.size());
	          cellList.clear();
	      }
	}

	@Override
	public boolean execute(int cellId)
	{
	    cellList.add(cellId);
		return true;
	}
	
	void partition(int numberOfPartitions,double MBR0, double MBR1,
			double MBR2, double MBR3, double [][]grid)
	{
			  switch(numberOfPartitions)
			  {
			   case 2:
			   partitionInto2cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 4:
			   partitionInto4Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 8:
			   partitionInto8cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 16:
			   partitionInto16Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 32:
			   partitionInto32Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 64:
			   partitionInto64Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 128:
			   partitionInto128Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 256:
			   partitionInto256Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 512:
			   partitionInto512Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   case 1024:
			   partitionInto1024Cells(0,MBR0, MBR1, MBR2, MBR3, grid);
			   break;
			   
			   default:
			   {
			    System.out.println("NOT SUPPORTED");
			   }
			 }
		}
			     
			void partitionInto2cells(int index,double lowX, double lowY, 
			double upX, double upY, double [][]grid)
			{
			 double deltaX = upX - lowX;
			 //calculate x-centre
			 double midX = lowX + deltaX/2;
			 
			 addToMBR(index+0,lowX, lowY, midX, upY, grid);
			 addToMBR(index+1,midX, lowY, upX, upY, grid);
			}

			void partitionInto4Cells(int index, double lowX, double lowY, double upX, double upY,
					double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 addToMBR(index+0,lowX, lowY, midX, midY, grid);
			 addToMBR(index+1,midX, lowY, upX, midY, grid);
			 addToMBR(index+2,lowX, midY, midX, upY, grid);
			 addToMBR(index+3,midX, midY, upX, upY, grid);
			}

			void partitionInto8cells(int index, double lowX, double lowY, double upX, double upY,
					double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 partitionInto2cells(index+0,lowX, lowY, midX, midY,grid);
			 partitionInto2cells(index+2,midX, lowY, upX, midY,grid);
			 partitionInto2cells(index+4,lowX, midY, midX, upY,grid);
			 partitionInto2cells(index+6,midX, midY, upX, upY,grid);
			}

			void partitionInto16Cells(int index,double lowX, double lowY, 
			double upX, double upY, double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 partitionInto4Cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto4Cells(index+4,midX, lowY, upX, midY, grid);
			 partitionInto4Cells(index+8,lowX, midY, midX, upY, grid);
			 partitionInto4Cells(index+12,midX, midY, upX, upY, grid);
			}

			void partitionInto32Cells(int index, double lowX, double lowY, double upX, double upY, 
					double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 partitionInto8cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto8cells(index+8,midX, lowY, upX, midY, grid);
			 partitionInto8cells(index+16,lowX, midY, midX, upY, grid);
			 partitionInto8cells(index+24,midX, midY, upX, upY, grid);
			}

			void partitionInto64Cells(int index,double lowX, double lowY, double upX, 
			double upY,double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 partitionInto16Cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto16Cells(index+16,midX, lowY, upX, midY, grid);
			 partitionInto16Cells(index+32,lowX, midY, midX, upY, grid);
			 partitionInto16Cells(index+48,midX, midY, upX, upY, grid);
			}

			void partitionInto128Cells(int index,double lowX, double lowY, double upX, 
			double upY,double [][]grid)
			{
			 double deltaX = upX - lowX;
			 //calculate x-centre
			 double midX = lowX + deltaX/2;
			 
			 partitionInto64Cells(index+0,lowX, lowY, midX, upY, grid);
			 partitionInto64Cells(index+64,midX, lowY, upX, upY, grid);
			}

			void partitionInto256Cells(int index,double lowX, double lowY, double upX, 
					double upY,double [][]grid)
			{
			 double deltaX = upX - lowX;
			 double deltaY = upY - lowY;
			 //calculate centre
			 double midX = lowX + deltaX/2;
			 double midY = lowY + deltaY/2;

			 partitionInto64Cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto64Cells(index+64,midX, lowY, upX, midY, grid);
			 partitionInto64Cells(index+128,lowX, midY, midX, upY, grid);
			 partitionInto64Cells(index+192,midX, midY, upX, upY, grid);
			}


			void partitionInto512Cells(int index,double lowX, double lowY, double upX, 
			double upY,double [][]grid)
			{
			  double deltaX = upX - lowX;
			  double deltaY = upY - lowY;
			  //calculate centre
			  double midX = lowX + deltaX/2;
			  double midY = lowY + deltaY/2;
			  
			  
			 partitionInto128Cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto128Cells(index+128,midX, lowY, upX, midY, grid);
			 partitionInto128Cells(index+256,lowX, midY, midX, upY, grid);
			 partitionInto128Cells(index+384,midX, midY, upX, upY, grid);
			}

			void partitionInto1024Cells(int index,double lowX, double lowY, double upX, 
			double upY,double [][]grid)
			{
			  double deltaX = upX - lowX;
			  double deltaY = upY - lowY;
			  //calculate centre
			  double midX = lowX + deltaX/2;
			  double midY = lowY + deltaY/2;
			  
			 partitionInto256Cells(index+0,lowX, lowY, midX, midY, grid);
			 partitionInto256Cells(index+256,midX, lowY, upX, midY, grid);
			 partitionInto256Cells(index+512,lowX, midY, midX, upY, grid);
			 partitionInto256Cells(index+768,midX, midY, upX, upY, grid);
			}

			void addToMBR(int index, double lowX, double lowY, double upX,   double upY,double [][]grid)
			 {
			  grid[index][0] = lowX;
			  grid[index][1] = lowY;
			  grid[index][2] = upX;
			  grid[index][3] = upY;
			 }
	
	
}
