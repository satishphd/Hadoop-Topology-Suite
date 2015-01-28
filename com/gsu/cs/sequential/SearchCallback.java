package com.gsu.cs.sequential;

import java.util.ArrayList;
import java.util.List;

import com.seisw.util.geom.Clip;
import com.seisw.util.geom.PolyDefault;

import gnu.trove.TIntProcedure;

public class SearchCallback implements TIntProcedure 
{
    public int clipPolyId;
    List<PolyDefault> outputList = new ArrayList<PolyDefault>();
    PolyDefault result;
    
	public boolean execute(int basePolyId) 
	{
		
		
		return false;
	}

}
