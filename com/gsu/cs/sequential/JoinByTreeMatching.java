package com.gsu.cs.sequential;


import java.awt.geom.Point2D;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;

import com.infomatiq.jsi.Rectangle;
import com.infomatiq.jsi.rtree.Node;
import com.infomatiq.jsi.rtree.RTree;
import com.seisw.util.geom.PolyDefault;

import gnu.trove.TIntProcedure;

public class JoinByTreeMatching implements TIntProcedure
{
 private int m_clipId;
 private static int queryTotal = 0;
 public static void main(String []args)
 {
	 
	 long start = System.nanoTime();
	 JoinByTreeMatching t = new JoinByTreeMatching();
//	 Rectangle qRect = new Rectangle(21.5f, 21.5f, 21.5f, 21.5f);
//	 Rectangle qRect1 = new Rectangle(1.5f, 1.5f, 1.5f, 1.5f);
//	 Rectangle qRect2 = new Rectangle(5.5f, 5.5f, 5.5f, 5.5f);
//	 Rectangle qRect3 = new Rectangle(2.5f, 2.5f, 2.5f, 2.5f);
//	 
	 //t.rtreeSearch(qRect3);
//	 System.out.println("Matching ");
//	 t.joinByMatching(args[0],args[1]);
	
	 System.out.println("Nested ");
	 t.nestedJoin(args[0],args[1]);
	 long end = System.nanoTime();
	 long elapsedTime = end - start;
	 double aseconds = (double)elapsedTime / 1000000000.0;
	 System.out.println(aseconds + "CPU in total (seconds) "+queryTotal);
 }
 
 private void rtreeSearch(Rectangle qRect)
 {
	 RTree tree = new RTree();
	 tree.init(null);
	 
	 Rectangle r1 = new Rectangle(6.0f, 6.0f, 7.0f,7.0f);
	 Rectangle r2 = new Rectangle(1.5f, 3.5f, 2.5f, 4.5f);
	 Rectangle r3 = new Rectangle(20.0f, 20.0f, 21.0f, 21.0f);
	 Rectangle r4 = new Rectangle(3.5f, 1.5f, 4.0f, 2.5f);
	 tree.add(r1,10);
	 tree.add(r2,11);
	 tree.add(r3,12);
	 tree.add(r4,13);

	 //*******************************************************************
	 RTree tree1 = new RTree();
	 tree1.init(null);
	 
	 Rectangle R1 = new Rectangle(1.0f, 1.0f, 2.0f, 2.0f);
	 Rectangle R2 = new Rectangle(1.0f, 3.0f, 2.0f, 4.0f);
	 Rectangle R3 = new Rectangle(3.0f, 1.0f, 4.0f, 2.0f);
	 Rectangle R4 = new Rectangle(3.0f, 3.0f, 4.0f, 4.0f);
	 tree1.add(R1,110);
	 tree1.add(R2,111);
	 tree1.add(R3,112);
	 tree1.add(R4,113);
	 
	 //Rectangle bounds = tree.getBounds();
	 //System.out.println("root node id " + tree.getRootNodeId());
	 Node root = tree.getNode(tree.getRootNodeId());
	 Node root1 = tree1.getNode(tree1.getRootNodeId());
	 joinRtree(root, root1, tree, tree1);
	 
 }
 
 private void joinByMatching(String b,String c)
 {
   RTree btree = new RTree();
   btree.init(null);
   ArrayList<PolyDefault> basePolys = new ArrayList<PolyDefault>();
   readBaseFile(b, basePolys, btree);
 
   RTree ctree = new RTree();
   ctree.init(null);
   ArrayList<PolyDefault> clipPolys = new ArrayList<PolyDefault>();
   readClipFile(c,clipPolys, ctree);
   
   Node broot = btree.getNode(btree.getRootNodeId());
   Node croot = ctree.getNode(ctree.getRootNodeId());
   joinRtree(broot,croot,btree,ctree);
 }
 
 public void nestedJoin(String b, String c)
 {
	 RTree btree = new RTree();
	 btree.init(null);
	 ArrayList<PolyDefault> basePolys = new ArrayList<PolyDefault>();
	 readBaseFile(b, basePolys, btree);
	 
	 ArrayList<PolyDefault> clipPolys = new ArrayList<PolyDefault>();
	 RTree ctree = new RTree();
	 ctree.init(null);
	 readClipFile(c,clipPolys,ctree);
	 
	 PolyDefault clipPoly;
	 for(int i = 0; i<clipPolys.size();i++)
	 {
		 clipPoly = clipPolys.get(i);
	     Rectangle clipBox = new Rectangle();
			
		 clipBox.maxX = (float)clipPoly.getM_ubBox().getX();
		 clipBox.maxY = (float)clipPoly.getM_ubBox().getY();
		 clipBox.minX = (float)clipPoly.getM_lbBox().getX();
		 clipBox.minY = (float)clipPoly.getM_lbBox().getY();
		 m_clipId = clipPoly.getId();
	    
		 btree.intersects(clipBox, this);
	 }
 }
 
 public void joinRtree(Node aNode, Node bNode,RTree t1, RTree t2)
 {
 	for(int i = 0; i< aNode.entryCount; i++)
 	{
 		for(int j = 0; j< bNode.entryCount; j++)
 		{
 			if(Rectangle.intersects(aNode.entriesMinX[i], aNode.entriesMinY[i], aNode.entriesMaxX[i], aNode.entriesMaxY[i],
		                            bNode.entriesMinX[j], bNode.entriesMinY[j], bNode.entriesMaxX[j], bNode.entriesMaxY[j]))
 			{
 			  if(aNode.isLeaf() && bNode.isLeaf())
 			  {
 				  //System.out.println("join " + aNode.ids[i] + "  " + bNode.ids[j]);
 				 queryTotal = queryTotal + 1;
 			  }
 			  else if(aNode.isLeaf())
 			  {
 				  joinRtree(aNode,t2.getNode(bNode.ids[j]),t1,t2);
 			  }
 			  else if(bNode.isLeaf())
 			  {
 				  joinRtree(t1.getNode(aNode.ids[i]),bNode,t1,t2);
 			  }
 			  else
 			  {
 				  joinRtree(t1.getNode(aNode.ids[i]), t2.getNode(bNode.ids[j]),t1,t2);
 			  }
 			}
 		}
 	}
 }
 
 public static void readBaseFile(String filename, ArrayList<PolyDefault> polylist, RTree tree)
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
 			//Read File Line By Line
 			while ((strLine = br.readLine()) != null) 	
 			{
 				 PolyDefault p1 = new PolyDefault();
 		         
 			     StringTokenizer itr = new StringTokenizer(strLine.toString());
 			      
 			      int count = 0;
 			      int id;
 			      //int vCount = (itr.countTokens() - 4)/2;
 			         
 			      while (itr.hasMoreTokens()) 
 			      {
 			    	if(count == 0)
 			    	{
 			    	 itr.nextToken(); //discard
 			         id = Integer.parseInt(itr.nextToken()); //index or line number	
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
 			      
 			      Rectangle r = new Rectangle((float)p1.getM_lbBox().getX(), (float)p1.getM_lbBox().getY(), (float)p1.getM_ubBox().getX(), 
 			    		  (float)p1.getM_ubBox().getY());
				 
 			      tree.add(r, p1.getId());
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
 
	public static void readClipFile(String filename, ArrayList<PolyDefault> polylist, RTree t)
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
	   			        Point2D vertex = new Point2D.Double(xCord,yCord);
	   			        
	   			        p1.add(vertex);
	   			      }
	   			      count = 0;
	   			      Rectangle r = new Rectangle((float)p1.getM_lbBox().getX(), (float)p1.getM_lbBox().getY(), (float)p1.getM_ubBox().getX(), 
	 			    		  (float)p1.getM_ubBox().getY());
					 
	 			      t.add(r, p1.getId());
	   			      polylist.add(p1); //Integer.toString(vCount
	   			      
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
 
	public static void readClipFile(String filename, ArrayList<PolyDefault> polylist)
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
	   			        Point2D vertex = new Point2D.Double(xCord,yCord);
	   			        
	   			        p1.add(vertex);
	   			      }
	   			      count = 0;
	   			      Rectangle r = new Rectangle((float)p1.getM_lbBox().getX(), (float)p1.getM_lbBox().getY(), (float)p1.getM_ubBox().getX(), 
	 			    		  (float)p1.getM_ubBox().getY());
					 
	   			      polylist.add(p1); //Integer.toString(vCount
	   			      
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

	public boolean execute(int arg0) 
	{
		// TODO Auto-generated method stub
		//System.out.println(arg0);
		queryTotal = queryTotal + 1;
		return true;
	}

}
