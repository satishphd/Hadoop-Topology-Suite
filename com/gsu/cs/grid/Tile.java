package com.gsu.cs.grid;

import java.awt.geom.Point2D;

public class Tile 
{
 Point2D low;
 Point2D high;
 Tile(double xLow, double yLow, double xHigh, double yHigh)
 {
	 low = new Point2D.Double(xLow, yLow);
	 high = new Point2D.Double(xHigh,yHigh);
 }
}
