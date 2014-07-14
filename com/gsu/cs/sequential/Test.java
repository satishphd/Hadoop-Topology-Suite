package com.gsu.cs.sequential;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.seisw.util.geom.PolyDefault;

public class Test
{
 public static void main(String[] args)
 {
	 
	 PolyDefault p1 = new PolyDefault();
     Point2D s1 = new Point2D.Double(11.0,6.0);
     Point2D s2 = new Point2D.Double(16.0,28.0);
     Point2D s3 = new Point2D.Double(10.0,40.0);
     Point2D s4 = new Point2D.Double(12.0,50.0);
     Point2D s5 = new Point2D.Double(14.0,45.0);
     Point2D s6 = new Point2D.Double(10.0,36.0);
     Point2D s7 = new Point2D.Double(10.0,8.0);
     p1.add(s1);
     p1.add(s2);
     p1.add(s3);
     p1.add(s4);
     p1.add(s5);
     p1.add(s6);
     p1.add(s7);
     
     p1.setM_lbBox(s7);
     p1.setM_ubBox(s2);
     
     PolyDefault p2 = new PolyDefault();
     Point2D c1 = new Point2D.Double(11.0,2.0);
     Point2D c2 = new Point2D.Double(18.0,4.0);
     Point2D c3 = new Point2D.Double(6.0,16.0);
     Point2D c4 = new Point2D.Double(12.0,26.0);
     Point2D c5 = new Point2D.Double(5.0,37.0);
     Point2D c6 = new Point2D.Double(11.0,2.0);
     Point2D c7 = new Point2D.Double(13.0,52.0);
     Point2D c8 = new Point2D.Double(16.0,36.0);
     Point2D c9 = new Point2D.Double(4.0,15.0);
     
     p2.add(c1);
     p2.add(c2);
     p2.add(c3);
     p2.add(c4);
     p2.add(c5);
     p2.add(c6);
     p2.add(c7);
     p2.add(c8);
     p2.add(c9);
     
     
     p2.setM_lbBox(c9);
     p2.setM_ubBox(c2);
     
     List<PolyDefault> list = new ArrayList<PolyDefault>();
     list.add(p1);
     list.add(p2);
     
     Parser parser = new Parser();
     parser.writeFile("C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\out1.txt",list);
     Collections.sort(list,new PolyLowXComparator());
     parser.writeFile("C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\out2.txt",list);
     
 }
}
 
 
