package com.gsu.cs.grid;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.infomatiq.jsi.Rectangle;

public class GridPartition 
{

	private Rectangle grid = new Rectangle();
	private float delX;
	private float delY;
	
	// handling square numbers, equal rows and columns
	GridPartition(float aLowerX, float aLowerY, float aUpperX, float aUpperY)
    {
    	grid.minX = aLowerX;
    	grid.minY = aLowerY;
    	grid.maxX = aUpperX;
    	grid.maxY = aUpperY;
    }
    
    public void computeXYIncrements(int rowCount, int columnCount)
    {
      float xRange = grid.maxX - grid.minX;
      delX = xRange/columnCount;
      
      float yRange = grid.maxY - grid.minY;
      delY = yRange/rowCount; 
    }
    
    public HashMap<Integer,Rectangle> computePartitions()
    {
    	int cellId = 1;
    	Rectangle cell;
    	//cell id mapped to cell boundary
        HashMap<Integer,Rectangle> cellTable = new HashMap<Integer,Rectangle>();
        
    	for(float y = grid.minY; y < grid.maxY; y = y + delY)
    	{
    		for(float x = grid.minX; x < grid.maxX; x = x + delX)
    		{
    			cell = new Rectangle(x,y,x+delX, y+delY);
    			cellTable.put(cellId,cell);
    			cellId = cellId + 1;
    		}
    	}
    	return cellTable;
    }
    
    public static void main(String[] args)
    {
    float lowerX = 1;
	float lowerY = 0.5f;
	float upperX = 10;
	float upperY = 8;
	GridPartition grid = new GridPartition(lowerX,lowerY,upperX,upperY);
	int rowCount = 2;
	int columnCount = 2;
	
    grid.computeXYIncrements(rowCount, columnCount);

	HashMap<Integer,Rectangle> cellTable = grid.computePartitions();
	grid.displayCells(cellTable);
}

//    public static void main(String[] args)
//    {
//    	float lowerX = 2;
//    	float lowerY = 1;
//    	float upperX = 10;
//    	float upperY = 6;
//    	
//    	GridPartition grid = new GridPartition(lowerX,lowerY,upperX,upperY);
//    	int rowCount = 2;
//    	int columnCount = 2;
//    	
//    	grid.computeXYIncrements(rowCount, columnCount);
//    
//    	HashMap<Integer,Rectangle> cellTable = grid.computePartitions();
//    	
//    	grid.displayCells(cellTable);
//    }

	private void displayCells(HashMap<Integer, Rectangle> cellTable) {
		Iterator it = cellTable.entrySet().iterator();
    	
        while (it.hasNext()) 
        {
            Map.Entry cell = (Map.Entry)it.next();
            Rectangle rect = (Rectangle)cell.getValue();
            System.out.println(cell.getKey() + "-> minX " + rect.minX + " minY " + rect.minY + " maxX " + rect.maxX + " maxY " + rect.maxY);
        }
	}


public Rectangle getGrid() {
	return grid;
}

public void setGrid(Rectangle grid) {
	this.grid = grid;
}

}
/*
if grid = 4 by 8 and partitions required = 4
    ****
    *  *
    *  *
    *  *
    *  *
    *  *
    *  *
    ****
*/