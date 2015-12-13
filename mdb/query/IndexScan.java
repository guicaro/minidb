package query;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import util.DBAccess;
import util.GenericTuple;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class IndexScan
{
	DBAccess dbInstance = DBAccess.getInstance();
	Map<String, Vector<String>> schemaMap =  dbInstance.getSchemaMap();
		
	private StoredClassCatalog classCatalog = dbInstance.getClassCatalog();
	Vector<String> Records=new Vector<String>();	
	GenericTuple tuple = null;
	Vector<Vector<String>> Allrecords= new Vector<Vector<String>>();
	
	
	public Vector<Vector<String>> process(Relation r)
	{
		Vector<LocalPred> localPreds1 = new Vector<LocalPred>();

		localPreds1= r.getLocalPreds();
		int pos=r.getScanAttrPosition();
		String relName=r.getRelationName();
		Database workDb = null;
		workDb = dbInstance.getDbHandle(relName);
		SecondaryDatabase secondaryDb = null;
		Transaction txn;
		txn = dbInstance.getActiveTransaction();
		Cursor cursor1=null;
		
		OperationStatus operStatus = null;		
		String attr=localPreds1.elementAt(pos).getAttribute();
		String operator=localPreds1.elementAt(pos).getOperator(); 
		String val=localPreds1.elementAt(pos).getValue();
		DatabaseEntry theData = new DatabaseEntry();
        Vector<String> columnInfo = new Vector<String>();
		columnInfo = (Vector) schemaMap.get(relName);	
		boolean qualifiedTuple=true;
				
		
		if(attr.trim().equals(columnInfo.elementAt(0).trim()))
				{
			try {
				
				cursor1=workDb.openCursor(txn, null);
				DatabaseEntry theKey = new DatabaseEntry(val.getBytes("UTF-8"));
			operStatus=cursor1.getSearchKey(theKey, theData, LockMode.DEFAULT);
			String str= new String(theKey.getData(), "UTF-8");
			if(operStatus==OperationStatus.SUCCESS)
			{
					tuple=dbInstance.retrieve(str, workDb, txn, LockMode.DEFAULT);
					Records=tuple.getTuple();
				
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
							pos+=3;

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
						
			cursor1.close();
				}
			
			catch (Exception e)
			{
				System.out.println(e);
			}
				}
				
		
		
		
		else
				{
					try
					{
					DatabaseEntry theKey = new DatabaseEntry(val.getBytes("UTF-8"));
					String Indexattr = "_ind_".concat(attr);
					secondaryDb=dbInstance.getIndexHandle(Indexattr);
					SecondaryCursor Cursor = secondaryDb.openSecondaryCursor(txn, null);
					operStatus=Cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
					while(operStatus==OperationStatus.SUCCESS)
						{
							EntryBinding dataBinding = new SerialBinding(classCatalog,GenericTuple.class);
							tuple = (GenericTuple) dataBinding.entryToObject(theData);
							Records=tuple.getTuple();
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
									pos+=3;

								}						
								
								if (qualifiedTuple == false) {
									break;

								}
								
							}
							
							if(qualifiedTuple)
							{
								
						Allrecords.add(Records);														
						operStatus=Cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
						}
							
						}
					Cursor.close();
				}
					catch(Exception e)
					{
						System.out.println("Exception" + e);
					}
					
								
								
				}
		
		return Allrecords;

		
	}
}
	

			
	
	

		

	

	
	