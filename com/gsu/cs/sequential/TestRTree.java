package com.gsu.cs.sequential;

import com.seisw.util.geom.*;
import gnu.trove.TIntProcedure;

import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;

import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.rtree.RTree;
import com.seisw.util.geom.PolyDefault;

public class TestRTree implements TIntProcedure
{
	Rectangle brect = new Rectangle();
	Rectangle crect = new Rectangle();
	
	private static ArrayList<PolyDefault> basePolyList = new ArrayList<PolyDefault>();
	private static ArrayList<PolyDefault> overlayPolyList = new ArrayList<PolyDefault>();
	private int clipPolyId;
	static int count = 0;
	public static void main(String []args)
	{
		String filename = args[0];
		TestRTree tree = new TestRTree();
		tree.main1(filename);
		System.out.println("count = " + count);
	}
	
	public void main1(String filename)
    {
		long seqbefore = System.currentTimeMillis();
	  //TIntProcedure callback = new SearchCallback();
	  Parser parser = new Parser();
	  RTree tree = new RTree();  
	  tree.init(null);
//	  parser.readBaseFileToTree("//Users//satishpuri//Desktop//GIS_stuff//basesamplelayer.txt",
//				basePolyList,tree);
//    "Users//satishpuri//Desktop//Midrange_files_hadoop//bases_242.txt"
	  parser.readBaseFile(filename, basePolyList);
	  //add rectangles to tree
	  long  start = System.nanoTime();
	  for(int i = 0; i< basePolyList.size(); i++)
	  {
		  Point2D lbox = basePolyList.get(i).getM_lbBox();
		  Point2D ubox = basePolyList.get(i).getM_ubBox();
		  Rectangle r2 = new Rectangle((float)lbox.getX(), (float)lbox.getY(), (float)ubox.getX(), (float)ubox.getY());
		  tree.add(r2, basePolyList.get(i).getId());
	  }
	  long end = System.nanoTime();
	  long elapsedTime = end - start;
	  double aseconds = (double)elapsedTime / 1000000.0;
	  System.out.println("Guttman tree size " + tree.size() + "time " + aseconds);
	  
	 
//	  parser.readClipFile("//Users//satishpuri//Desktop//GIS_stuff//overlaysamplelayer.txt",
//				  overlayPolyList);
//	  parser.readClipFile("//Users//satishpuri//Desktop//Midrange_files_hadoop//overlay_300.txt",
//			  overlayPolyList);
//	  
//	  PolyDefault clipPoly = null;
//	  Rectangle clipBox = new Rectangle();
//	  
//	  for(int count = 0; count < overlayPolyList.size(); count++)
//	  {
//		  clipPoly = overlayPolyList.get(count);
//		  clipBox.maxX = (float)clipPoly.getM_ubBox().getX();
//		  clipBox.maxY = (float)clipPoly.getM_ubBox().getY();
//		  clipBox.minX = (float)clipPoly.getM_lbBox().getX();
//		  clipBox.minY = (float)clipPoly.getM_lbBox().getY();
//		  this.clipPolyId = clipPoly.getId();
//		  //((SearchCallback)callback).clipPolyId = clipPoly.getId();
//		  //tree.intersects(clipBox, callback);
//		  tree.intersects(clipBox, this);
//	  }	
	  long seqafter = System.currentTimeMillis();

		System.out.println("Sequential R-tree construction time : "  + (seqafter - seqbefore) + "  " + count);
   }


	@Override
	public boolean execute(int basePolyId)
	{
		//Poly out = Clip.intersection(basePolyList.get(basePolyId), overlayPolyList.get(clipPolyId));
		//System.out.println(clipPolyId + " " + basePolyId );
//		if(!out.isEmpty())
		
		
		 brect.set((float)basePolyList.get(basePolyId).getM_lbBox().getX(),
				(float)basePolyList.get(basePolyId).getM_lbBox().getY(),
				(float)basePolyList.get(basePolyId).getM_ubBox().getX(),
				(float)basePolyList.get(basePolyId).getM_ubBox().getY());
		
		PolyDefault p = overlayPolyList.get(clipPolyId);
		
		crect.set((float)p.getM_lbBox().getX(),
				(float)p.getM_lbBox().getY(),
				(float)p.getM_ubBox().getX(), 
				(float)p.getM_ubBox().getY());
//		//System.out.println(basePolyId + " "+ clipPolyId);
		if(brect.intersects(crect))
			count = count + 1;
		else
		{
			System.out.println(brect);
			System.out.println(crect);
		}
		return true;
	}
}
