package com.gsu.cs.sequential;



import java.util.Comparator;

import com.seisw.util.geom.PolyDefault;
//this class is not used now
public class PolyUppXComparator implements Comparator<PolyDefault>
{
	@Override
	public int compare(PolyDefault poly1, PolyDefault poly2)
	{
	        /*
	         * parameter are of type Object, so we have to downcast it
	         * to Employee objects
	         */
	        double upperX1 = ((PolyDefault)poly1).getM_ubBox().getX();        
	        double upperX2= ((PolyDefault)poly2).getM_ubBox().getX();
	       
	        if(upperX1 > upperX2)
	            return 1;
	        else if(upperX1 < upperX2)
	            return -1;
	        else
	            return 0;    
	  }
	   
}

