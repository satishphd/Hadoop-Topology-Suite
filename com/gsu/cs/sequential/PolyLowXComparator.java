package com.gsu.cs.sequential;

import java.util.Comparator;

import com.seisw.util.geom.PolyDefault;
//this class is used
public class PolyLowXComparator implements Comparator<PolyDefault>
{
	@Override
	 public int compare(PolyDefault poly1, PolyDefault poly2)
	{
	        /*
	         * parameter are of type Object, so we have to downcast it
	         * to Employee objects
	         */
	       
	        double lowerX1 = poly1.getM_lbBox().getX();        
	        double lowerX2= poly2.getM_lbBox().getX();
	       
	        if(lowerX1 > lowerX2)
	            return 1;
	        else if(lowerX1 < lowerX2)
	            return -1;
	        else
	            return 0;    
	    }
	   
}
