package com.gsu.cs.sequential;

import java.util.Collections;
import java.util.List;

import com.seisw.util.geom.PolyDefault;

public class RelationshipGraph
{

	  public static void main(String a[])
	  {
	  int i;
	  double array[] = {-23.223,12.3332,9.5656,4.2134,99.2111,120.231,-1.00,-3.6787,0.333,13.22344};

	  
	  for(i = 0; i < array.length; i++)
	  System.out.print( array[i]+"  ");
	  System.out.println();
	  
	  //quick_srt(array,0,array.length-1);
	  System.out.print("Values after the sort:\n");
	  
	  for(i = 0; i <array.length; i++)
	  System.out.print(array[i]+"  ");
	  
	  }
	  
	  public static void createRelationships(List<PolyDefault> basePolygon, List<PolyDefault> overlayPolygons)
      {
          double upperBaseX = 0.0;
          int range;
          for (int i = 0; i < basePolygon.size(); i++)
          {
              upperBaseX= basePolygon.get(i).getM_ubBox().getX();
              /* Find the polygon with index where base's upperBox is no less than overlay's lower box */
              range = BinarySearch(overlayPolygons, upperBaseX);
              //List<PolyDefault> remaining = overlayPolygons.GetRange(0, range);
              List<PolyDefault> remaining = overlayPolygons.subList(0, range);
              for(PolyDefault p: remaining)
              { 
                  if (isOverlap(basePolygon.get(i), p))
                  {
                      basePolygon.get(i).linkPolygon(p);
                  }
              }
          }
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

      public static int BinarySearch(List<PolyDefault> polygons, double maxXPoint)
      {
          int left = 0;
          int right = polygons.size();

          int middle = 0;
          while (left < right)
          {

              middle = (left + right) / 2;
              /*Find the first polygon whose lowerbox is greater than or equal to the upper of base */

              /* if the upper of base is less than lower of overlay, ignore right part */
              
              //if (maxPoint < polygons[middle].lowerBBox.X)
              if(maxXPoint < polygons.get(middle).getM_lbBox().getX())
              {
                  right = middle - 1;
              }

              /* otherwise, left can be expanded*/
              //else if (maxPoint >= polygons[middle].lowerBBox.X)
              else if(maxXPoint >= polygons.get(middle).getM_lbBox().getX())
              {
                  left = middle + 1;
              }
          }

          return middle;
      }

}

	 /*public static void quick_srt(List<PolyDefault> list,int low, int n)
	 {
	  int lo = low;
	  int hi = n;
	  if (lo >= n)
	  {
	   return;
	  }
	  int mid = (lo+hi)/2;
	  PolyDefault midPoly = list.get(mid);  //double mid = array[(lo + hi) / 2];
	  while (lo < hi) 
	  {
	    while (lo<hi && list.get(lo).getM_lbBox().getX() < midPoly.getM_lbBox().getX()) // while (lo<hi && array[lo] < mid)
	    {
	     lo++;
	    }
	    
	    while (lo<hi &&  list.get(hi).getM_lbBox().getX() > midPoly.getM_lbBox().getX()) // while (lo<hi && array[hi] > mid) 
	    {
	     hi--;
	    }
	    
	    //swapping polygons
	    if (lo < hi) 
	    {
	     Collections.swap(list,low,hi);
	    }
	  }
	  if (hi < lo) 
	  {
	   int T = hi;
	   hi = lo;
	   lo = T;
	  }
	  quick_srt(list, low, lo); //quick_srt(array, low, lo);
	  quick_srt(list, lo == low ? lo+1 : lo, n);  //quick_srt(array, lo == low ? lo+1 : lo, n);
	  
	  }
}


public static void main(String a[])
{
int i;
double array[] = {-23.223,12.3332,9.5656,4.2134,99.2111,120.231,-1.00,-3.6787,0.333,13.22344};


for(i = 0; i < array.length; i++)
System.out.print( array[i]+"  ");
System.out.println();

quick_srt(array,0,array.length-1);
System.out.print("Values after the sort:\n");

for(i = 0; i <array.length; i++)
System.out.print(array[i]+"  ");

}
public static void quick_srt(double array[],int low, int n)
	  {
	  int lo = low;
	  int hi = n;
	  if (lo >= n)
	  {
	   return;
	  }
	  
	  double mid = array[(lo + hi) / 2];
	  
	  while (lo < hi) 
	  {
	   while (lo<hi && array[lo] < mid)
	   {
	    lo++;
	   }
	   while (lo<hi && array[hi] > mid) 
	   {
	    hi--;
	   }
	   if (lo < hi) 
	   {
	    double T = array[lo];
	    array[lo] = array[hi];
	    array[hi] = T;
	   }
	  }
	  if (hi < lo) 
	  {
	   int T = hi;
	   hi = lo;
	   lo = T;
	  }
	  quick_srt(array, low, lo);
	  quick_srt(array, lo == low ? lo+1 : lo, n);
	  }
	}

*/