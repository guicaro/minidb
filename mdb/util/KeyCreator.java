package util;

import java.io.UnsupportedEncodingException;
import java.util.Vector;
import util.GenericTuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.DatabaseEntry;;

public class KeyCreator implements SecondaryKeyCreator {
	
	int pos;
	private EntryBinding  theBinding;
	
	public KeyCreator (EntryBinding  theBinding1, int attributePosition)
	{
		pos = attributePosition;
		theBinding = theBinding1;
	}
	
	public boolean createSecondaryKey(SecondaryDatabase secDb, DatabaseEntry keyEntry, 
			                         DatabaseEntry dataEntry, DatabaseEntry resultEntry)
	{
		
		GenericTuple retTuple = new GenericTuple();
		
		retTuple = (GenericTuple) theBinding.entryToObject(dataEntry);
		Vector<String> recordValues = new Vector<String>();
		recordValues = (Vector) retTuple.getTuple().clone();
		
		String newKey = recordValues.elementAt(pos);				
		try {
			resultEntry.setData(newKey.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
		}
		return true;
			
	}

}
