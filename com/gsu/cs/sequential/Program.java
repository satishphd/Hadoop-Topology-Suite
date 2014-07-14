package com.gsu.cs.sequential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

public class Program 
{
	private static ArrayList<PolyDefault> basePolyList = new ArrayList<PolyDefault>();
	private static ArrayList<PolyDefault> overlayPolyList = new ArrayList<PolyDefault>();
	
 
	
	public static void main(String[] args)
	 {
		Parser parser = new Parser();
		//String basePath = "C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\Midrange_files_hadoop\\bases_242.txt";
		//String clipPath = "C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\Midrange_files_hadoop\\overlay_300.txt";
		//String mergedPath = "C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\Midrange_files_hadoop\\mergedFile.txt";
		
		//parser.mergeFiles(basePath,clipPath,mergedPath);
		  
//		parser.readBaseFile("//Users//satishpuri//Desktop//random//p10000-0",
//			basePolyList);
		
		parser.readBaseFile("//Users//satishpuri//Desktop//Midrange_files_hadoop//bases_242.txt",
				basePolyList); 
//		  parser.readFile("C:\\Users\\satish\\Desktop\\hadoop\\overlaysamplelayer.txt",
//			overlayPolyList);
		  parser.readClipFile("//Users//satishpuri//Desktop//Midrange_files_hadoop//overlay_300.txt",
				  overlayPolyList);
		  //System.out.println("Overlay poly count" + overlayPolyList.size());
		  //System.out.println("Base poly count " + basePolyList.size());

		//Collections.sort(overlayPolyList,new PolyLowXComparator());
		//parser.writeFile("C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\sortLower.txt", basePolyList);
		//System.out.println("Sorting overlay layer polygons over");
        //RelationshipGraph.createRelationships(basePolyList, overlayPolyList);
        //System.out.println("relationship done");
        /*for(int i = 0; i< basePolyList.size();i++)
        {
        	if(basePolyList.get(i).getOverlayPoly().size() > 0)
        		System.out.println("base poly index = " + i + " = "+basePolyList.get(i).getOverlayPoly().size() );
        }*/
        List<PolyDefault> outputList = new ArrayList<PolyDefault>();
        PolyDefault result,base,clip; 
        int overlayPolySize;
        //for(int i = 0; i< basePolyList.size();i++)
        String baseStr, clipStr;
        long before = System.currentTimeMillis();
        for(int i = 0; i< 1;i++)
        {
        	//if(i%1000 == 0)
        		//System.out.println("oye");
        	for(int j=0;j<overlayPolyList.size();j++)
        	{
        		base = basePolyList.get(i);
        		clip = overlayPolyList.get(j);
        		boolean overlapping = isOverlap(base,clip);
        		if(overlapping)
        		{
        			
        		  result = (PolyDefault)Clip.intersection(base, 
        				  clip);

        		  
        		  if(result.isEmpty() == false && result.getNumPoints() > 50)
        		  {
        			if(basePolyList.get(i).getNumPoints() != overlayPolyList.get(j).getNumPoints()) 
        			{
        		   //outputList.add(result);
        		   //baseStr = Parser.serializePolyStr(basePolyList.get(i));
        		   //clipStr = Parser.serializePolyStr(overlayPolyList.get(j));
        		   System.out.println("Base  " + basePolyList.get(i).getNumPoints()+"  " + overlayPolyList.get(j).getNumPoints() + "  " + result.getNumPoints());
        		   //System.out.println(baseStr);
        		   
        		   //System.out.println(clipStr);
        		   //System.out.println("Clip  " + overlayPolyList.get(j).getNumPoints());
        		   //System.out.println("Result " + result.getNumPoints());
        		   //break;
        			}
        		   //parser.writeFile("//Users//satishpuri//GIS//output.txt", resultStr);
        		  }
        		}
        	}
        	}
        long after = System.currentTimeMillis();
        System.out.println("Overlay time = " + (after - before));
        }
        
		
		
	 
	
	public static boolean isOverlap(PolyDefault p1, PolyDefault p2)
    {
        //if (p2.lowerBBox.X > p1.upperBBox.X)
		    if(p2.getM_lbBox().getX() > p1.getM_ubBox().getX())
            return false;
        
		    //if (p2.lowerBBox.Y > p1.upperBBox.Y)
		    if(p2.getM_lbBox().getY() > p1.getM_ubBox().getY())
            return false;

        //if (p2.upperBBox.X < p1.lowerBBox.X)
		    if(p2.getM_ubBox().getX() < p1.getM_lbBox().getX())
            return false;
		    
        //if (p1.lowerBBox.Y > p2.upperBBox.Y)
		    if(p1.getM_lbBox().getY() > p2.getM_ubBox().getY())
            return false;

          return true;
    }
	
	 private static List<PolyDefault> copyPolygons(ArrayList<PolyDefault> list, int count)
	 {
		 List<PolyDefault> localList = new ArrayList<PolyDefault>(); 
		 for(int i = 0;i<count; i++)
		 {
			 localList.add(list.get(i));
		 }
		 return localList;
	 }
}

 
