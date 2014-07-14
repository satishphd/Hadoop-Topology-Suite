package com.gsu.cs.sequential;

import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

import com.infomatiq.jsi.Rectangle;
import com.seisw.util.geom.Poly;
import com.seisw.util.geom.PolyDefault;
import com.infomatiq.jsi.rtree.RTree;

public class Parser 
{
	
	public static void mergeFiles(String baseFile,String overlayFile,String mergedFile)
	{
		ArrayList<String> basePolyList = new ArrayList<String>();
		try
		 {
				// Open the file that is the first 
				// command line parameter
				FileInputStream fstream = new FileInputStream(baseFile);
				// Get the object of DataInputStream
				DataInputStream in = new DataInputStream(fstream);
		        BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String strLine;
				//Read File Line By Line
				while ((strLine = br.readLine()) != null) 	
				{
					basePolyList.add(strLine); 
				}
				//Close the input stream
				in.close();
		  }
		  catch (Exception e)
		  {
			    //Catch exception if any
				System.err.println("Error: " + e.getMessage());
		  }
		  
		  
		  ArrayList<String> clipPolyList = new ArrayList<String>();
			try
			 {
					// Open the file that is the first 
					// command line parameter
					FileInputStream fstream = new FileInputStream(overlayFile);
					// Get the object of DataInputStream
					DataInputStream in = new DataInputStream(fstream);
			        BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String strLine;
					//Read File Line By Line
					while ((strLine = br.readLine()) != null) 	
					{
						clipPolyList.add(strLine); 
					}
					//Close the input stream
					in.close();
			  }
			  catch (Exception e)
			  {
				    //Catch exception if any
					System.err.println("Error: " + e.getMessage());
			  }
			  
			  try
			  {
			   FileWriter fstream = new FileWriter(mergedFile);
			   BufferedWriter out = new BufferedWriter(fstream);
			  
			   Iterator<String> baseItr = basePolyList.iterator();
			   Iterator<String> clipItr = clipPolyList.iterator();
			  
			   while(baseItr.hasNext() && clipItr.hasNext())
			   {  
				  out.write(baseItr.next());
				  out.write(clipItr.next());  
			   }
			   
			   while(baseItr.hasNext())
			   {
				   out.write(baseItr.next());
			   }
			   
			   while(clipItr.hasNext())
			   {
				   out.write(clipItr.next());
			   }
			   out.close();
			  }
			  catch(IOException ex)
			  {
				System.out.println("Error while writing merged file");  
			  }
			 
	}
	
 //private static ArrayList<PolyDefault> basePolyList = new ArrayList<PolyDefault>();
 //private static ArrayList<PolyDefault> overlayPolyList = new ArrayList<PolyDefault>();
 public static void main(String[] args)
 {
	 /*
	Parser parser = new Parser();
	parser.readFile("C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\basegml.txt",
			basePolyList); 
	parser.readFile("C:\\Users\\satish\\Desktop\\Spring 2012\\GIS\\parser\\overlaygml.txt",
			overlayPolyList);
	System.out.println(overlayPolyList.size());
	  System.out.println(basePolyList.size());*/
 }

 public void readBaseFileToTree(String filename, ArrayList<PolyDefault> polylist,RTree tree)
 {
	 try
	 {
			// Open the file that is the first 
			// command line parameter
			FileInputStream fstream = new FileInputStream(filename);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			String strId;
			int id;
			//Read File Line By Line
			while ((strLine = br.readLine()) != null) 	
			{
				 PolyDefault p1 = new PolyDefault();
		         
			     StringTokenizer itr = new StringTokenizer(strLine.toString());
			      
			      int count = 0;
			      //int vCount = (itr.countTokens() - 4)/2;
			         
			      while (itr.hasMoreTokens()) 
			      {
			    	if(count == 0)
			    	{
			    	 itr.nextToken(); // temp change  
		             strId = itr.nextToken(); 
		             id = Integer.parseInt(strId); 
		             id = id - 1;
		             p1.setId(id);
			         Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken())); 
			         p1.setM_lbBox(lbox);
			      
			         Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
			         p1.setM_ubBox(ubox);
			         //Rectangle r2 = new Rectangle(r.minX, r.minY, r.maxX, r.maxY);
			         Rectangle r2 = new Rectangle((float)lbox.getX(), (float)lbox.getY(), (float)ubox.getX(), (float)ubox.getY());
			         
			         tree.add(r2, id);
			    	}
			    	count = count + 1;
			    	
			        double xCord,yCord;
			      
			        xCord = Double.parseDouble(itr.nextToken());
			        yCord = Double.parseDouble(itr.nextToken());
			        //Point2D vertex = new Point2D.Double(xCord,yCord);
			        
			        //p1.add(vertex);
			      }
			      count = 0;
			      
			      polylist.add(p1); //Integer.toString(vCount)
			}
			//Close the input stream
			in.close();
	  }
	  catch (Exception e)
	  {
		    //Catch exception if any
			System.err.println("Error: " + e.getMessage());
	  }
	  
 }

 public void readClipFile(String filename, ArrayList<PolyDefault> polylist)
 {
	 try
	 {
			// Open the file that is the first 
			// command line parameter
			FileInputStream fstream = new FileInputStream(filename);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			String strId;
			int id;
			//Read File Line By Line
			while ((strLine = br.readLine()) != null) 	
			{
				 PolyDefault p1 = new PolyDefault();
		         
			     StringTokenizer itr = new StringTokenizer(strLine.toString());
			      
			      int count = 0;
			      //int vCount = (itr.countTokens() - 4)/2;
			         
			      while (itr.hasMoreTokens()) 
			      {
			    	if(count == 0)
			    	{
			    	 strId = itr.nextToken(); 
			         id = Integer.parseInt(strId); 
			         p1.setId(id);	
			         Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken())); 
			         p1.setM_lbBox(lbox);
			      
			         Point2D ubox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken()));
			         p1.setM_ubBox(ubox);
			    	}
			    	count = count + 1;
			    	
			        double xCord,yCord;
			      
			        xCord = Double.parseDouble(itr.nextToken());
			        yCord = Double.parseDouble(itr.nextToken());
			       // Point2D vertex = new Point2D.Double(xCord,yCord);
			        
			       // p1.add(vertex);
			      }
			      count = 0;
			      
			      polylist.add(p1); //Integer.toString(vCount)
			}
			//Close the input stream
			in.close();
	  }
	  catch (Exception e)
	  {
		    //Catch exception if any
			System.err.println("Error: " + e.getMessage());
	  }
	  
 }

 
 public void readBaseFile(String filename, ArrayList<PolyDefault> polylist)
 {
	 try
	 {
			// Open the file that is the first 
			// command line parameter
			FileInputStream fstream = new FileInputStream(filename);
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
	        BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			String strId;
			int id;
			//Read File Line By Line
			while ((strLine = br.readLine()) != null) 	
			{
				 PolyDefault p1 = new PolyDefault();
		         
			     StringTokenizer itr = new StringTokenizer(strLine.toString());
			      
			      int count = 0;
			      //int vCount = (itr.countTokens() - 4)/2;
			         
			      while (itr.hasMoreTokens()) 
			      {
			    	if(count == 0)
			    	{
			    	 itr.nextToken(); //discard
			         strId = itr.nextToken(); //index or line number	
			         id = Integer.parseInt(strId);
			         p1.setId(id);
			         Point2D lbox = new Point2D.Double(Double.parseDouble(itr.nextToken()),Double.parseDouble(itr.nextToken())); 
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
			      }
			      count = 0;
			      
			      polylist.add(p1); //Integer.toString(vCount)
			}
			//Close the input stream
			in.close();
	  }
	  catch (Exception e)
	  {
		    //Catch exception if any
			System.err.println("Error: " + e.getMessage());
	  }
	  
 }
 
  
 public void writeFile(String filename, List<PolyDefault> list)
 {
	 try{
		  // Create file 
		  FileWriter fstream = new FileWriter(filename);
		  BufferedWriter out = new BufferedWriter(fstream);
		  //out.write("Hello Java");
		  
		  PolyDefault poly;
		  for(int i=0;i<list.size();i++)
		  {
			  poly = list.get(i);
			  out.write(poly.getM_lbBox().getX() + " "+poly.getM_lbBox().getY() + " ");
			  out.write(poly.getM_ubBox().getX() + " "+poly.getM_ubBox().getY());
			  out.newLine();
		  }
		  //Close the output stream
		  out.close();
		  }catch (Exception e){//Catch exception if any
		  System.err.println("Error: " + e.getMessage());
		  }
 }
 
 public static Text serializePoly(PolyDefault poly)
 {
	 StringBuffer strBuf = new StringBuffer();
	 int innerPolyCnt = poly.getNumInnerPoly();
	 for(int loop = 0; loop < innerPolyCnt; loop++)
	 {	 
	   Poly ip = poly.getInnerPoly(loop);
	   for ( int i= 0; i < ip.getNumPoints(); i++)
       {
        double x = ip.getX(i);
        double y = ip.getY(i);
        strBuf.append(Double.toString(x) + " " + Double.toString(y) );
       }
	   strBuf.append("\n");
	 }
	 Text text = new Text(strBuf.toString());
	 return text;
 }
 
 public static String serializePolyStr(PolyDefault poly)
 {
	 StringBuffer strBuf = new StringBuffer();
	 int innerPolyCnt = poly.getNumInnerPoly();
	 for(int loop = 0; loop < innerPolyCnt; loop++)
	 {	 
	   Poly ip = poly.getInnerPoly(loop);
	   for ( int i= 0; i < ip.getNumPoints(); i++)
       {
        double x = ip.getX(i);
        double y = ip.getY(i);
        strBuf.append(Double.toString(x)+ " " + Double.toString(y) ).append(" ");
       }
	   strBuf.append("\n");
	 }
	 
	 return strBuf.toString();
 }
}
