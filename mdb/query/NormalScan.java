package query;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import mdb.AstCursor;

import util.DBAccess;
import util.GenericTuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class NormalScan
{
DBAccess dbInstance = DBAccess.getInstance();
Map<String, Vector<String>> schemaMap =  dbInstance.getSchemaMap();
StoredClassCatalog myClassCatalog = dbInstance.getClassCatalog();
private Database classCatalogDb;
GenericTuple tuple = null;
Vector<Vector<String>> Allrecords=new Vector<Vector<String>>();

public Vector<Vector<String>> process(Relation r)
{
	Vector<LocalPred> localPreds1 = new Vector<LocalPred>();

	localPreds1= r.getLocalPreds();
	String relName=r.getRelationName();
	Database workDb = null;
	workDb = dbInstance.getDbHandle(relName);
	SecondaryDatabase secondaryDb = null;
	Transaction txn;
	txn = dbInstance.getActiveTransaction();
	OperationStatus operStatus = null;		
	DatabaseEntry theData = new DatabaseEntry();
    Vector<String> columnInfo = new Vector<String>();
	columnInfo = (Vector) schemaMap.get(relName);	
	boolean qualifiedTuple = false;
		
	GenericTuple schemaTuple = new GenericTuple();
	
	Cursor cursor = null;

	
	try {
		cursor = workDb.openCursor(txn, null);
		
		EntryBinding dataBinding = new SerialBinding(myClassCatalog,GenericTuple.class);
		GenericTuple retTuple = new GenericTuple();

		// Get the DatabaseEntry objects that the cursor will use.
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
			if (localPreds1.size()==0) {
				// Set the flag for qualifedTupple since there are no local
				// predicates
				// to apply.
				qualifiedTuple = true;
			}
			
			retTuple = (GenericTuple) dataBinding.entryToObject(foundData);

			// Vector contains data in form: | Data | Type | Data | Type |
			// ...
			Vector<String> Records = new Vector<String>();
			Records = (Vector) retTuple.getTuple().clone();
			
			for (int x=0; x<localPreds1.size();x++)
			{
				int pos1=0;
				// Process comparisons on predicates for Integer
				// values
				for (int y = 0; y < Records.size(); y ++)

				{							
				// Process comparisons on String values
				if (columnInfo.elementAt(pos1).equalsIgnoreCase(localPreds1.elementAt(x).getAttribute()))
							 {
						if (localPreds1.elementAt(x).getOperator().equals("=")) {
							if (((String) Records.elementAt(y))
									.equalsIgnoreCase(localPreds1.elementAt(x).getValue()))
											
								qualifiedTuple = true;
							else
								qualifiedTuple = false;
						}  else if (localPreds1.elementAt(x).getOperator().equals("<=")) {
							if (Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) < 0
									|| Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) == 0)
								qualifiedTuple = true;
							else
								qualifiedTuple = false;
						} 
						else if (localPreds1.elementAt(x).getOperator().equals(">=")) {
							if (Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) > 0
									|| Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) == 0)
								qualifiedTuple = true;
							else
								qualifiedTuple = false;
						} 
						else if (localPreds1.elementAt(x).getOperator().equals(">")) {
							if (Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) > 0)
								qualifiedTuple = true;
							else
								qualifiedTuple = false;

						}
						else if (localPreds1.elementAt(x).getOperator().equals("<")) {
							if (Records.elementAt(y).compareToIgnoreCase(localPreds1.elementAt(x).getValue()) < 0)
								qualifiedTuple = true;
							else
								qualifiedTuple = false;
						}
						
						else if (localPreds1.elementAt(x).getOperator().equals("!=")) {
							if (!((String) Records.elementAt(y))
									.equalsIgnoreCase(localPreds1.elementAt(x).getValue()))
											
								qualifiedTuple = true;
							else
								qualifiedTuple = false;
						}

						// Already made comparison, and do not want
						// to compare
						// subsequent columns, go to next predicate.
						break;

					}
				pos1+=3;					

				}	
				
				if (qualifiedTuple == false) {
					break;

				}
						
			}
					
			
			if(qualifiedTuple)
			{
				Allrecords.add(Records);
			}
		}
		cursor.close();
		
	}
		
	catch(Exception e)
	{
		System.out.println("Exception" + e);
	}
	
	return Allrecords;
}

}
				
			