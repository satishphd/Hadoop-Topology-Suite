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

	public static void partition(int numberOfPartitions,double MBR0, double MBR1,
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

	private static void partitionInto2cells(int index,double lowX, double lowY, 
			double upX, double upY, double [][]grid)
	{
		double deltaX = upX - lowX;
		//calculate x-centre
		double midX = lowX + deltaX/2;

		addToMBR(index+0,lowX, lowY, midX, upY, grid);
		addToMBR(index+1,midX, lowY, upX, upY, grid);
	}

	private static void partitionInto4Cells(int index, double lowX, double lowY, double upX, 
			double upY, double [][]grid)
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

	private static void partitionInto8cells(int index, double lowX, double lowY, double upX,
			double upY, double [][]grid)
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

	private static void partitionInto16Cells(int index,double lowX, double lowY, 
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

	private static void partitionInto32Cells(int index, double lowX, double lowY, double upX, double upY, 
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

	private static void partitionInto64Cells(int index,double lowX, double lowY, double upX, 
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

	private static void partitionInto128Cells(int index,double lowX, double lowY, double upX, 
			double upY,double [][]grid)
	{
		double deltaX = upX - lowX;
		//calculate x-centre
		double midX = lowX + deltaX/2;

		partitionInto64Cells(index+0,lowX, lowY, midX, upY, grid);
		partitionInto64Cells(index+64,midX, lowY, upX, upY, grid);
	}

	private static void partitionInto256Cells(int index,double lowX, double lowY, double upX, 
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


	private static void partitionInto512Cells(int index,double lowX, double lowY, double upX, 
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

	private static void partitionInto1024Cells(int index,double lowX, double lowY, double upX, 
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

	private static void addToMBR(int index, double lowX, double lowY, double upX,   double upY,double [][]grid)
	{
		grid[index][0] = lowX;
		grid[index][1] = lowY;
		grid[index][2] = upX;
		grid[index][3] = upY;
	}

	public Rectangle getGrid() 
	{
		return grid;
	}

	public void setGrid(Rectangle grid) 
	{
		this.grid = grid;
	}
}