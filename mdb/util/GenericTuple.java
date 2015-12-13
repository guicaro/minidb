package util;

import java.io.Serializable;
import java.util.Vector;

public class GenericTuple implements Serializable {

	private Vector tuple = new Vector();
	
	public void setTuple(Vector v) {

		tuple.removeAllElements();
		
		for(int x=0; x<v.size(); x++)
		{
			tuple.add(v.elementAt(x));
		}
 	}

	public Vector getTuple() {
	return tuple;
	}
	
}
