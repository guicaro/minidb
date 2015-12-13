package util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;

public class DBAccess implements DBUtil {

	// The private reference to the one and only instance.
	private static DBAccess DBInstance = null;

	private Environment myEnv= null;
	private Database classCatalogDb;
	private Map activeDbMap = new HashMap();
	private Map activeIndexMap = new HashMap();
	private Map schemaMap = new HashMap();
	private Db dbHandle;
	private Index indexHandle;
	private Transaction txn = null;
	private StoredClassCatalog classCatalog;
		
	public boolean isIndexed(String relName, String columnName)
	{
		Vector<String> columnInfo = new Vector<String>();
	
		if (schemaMap.containsKey(relName)) {			
			columnInfo = (Vector) schemaMap.get(relName);
			
			for(int x=0; x<columnInfo.size(); x+=3)
			{
				if(relName.concat(".").concat(columnName).equals(columnInfo.elementAt(x)))
				{
					if(columnInfo.elementAt(x+2).equals("1"))
						return true;
				}
			}	
		}
		
		return false;
	
	}
	
	public boolean isIndexed(String relAttribute)
	{
		String relName = relAttribute.substring(0, relAttribute.indexOf("."));
		String columnName = relAttribute.substring(relAttribute.indexOf(".")+1, relAttribute.length());
			
		Vector<String> columnInfo = new Vector<String>();
	
		if (schemaMap.containsKey(relName)) {			
			columnInfo = (Vector) schemaMap.get(relName);
			
			for(int x=0; x<columnInfo.size(); x+=3)
			{
				if(relName.concat(".").concat(columnName).equals(columnInfo.elementAt(x)))
				{
					if(columnInfo.elementAt(x+2).equals("1"))
						return true;
				}
			}	
		}
		
		return false;
	
	}
	
	public boolean createSecondaryDb(String relName, String column, int attrPos)
	{
		boolean created = false;
		
		// Create secondary database
		Database primaryDb = getDbHandle(relName);

		EntryBinding dataBinding = new SerialBinding(classCatalog,
				GenericTuple.class);
		
		KeyCreator kc = new KeyCreator(dataBinding, attrPos);
		SecondaryConfig mySecConfig = new SecondaryConfig();
		mySecConfig.setKeyCreator(kc);
		mySecConfig.setAllowPopulate(true);
		// Need to allow duplicates for our secondary database
		mySecConfig.setSortedDuplicates(true);
		mySecConfig.setTransactional(true);
		mySecConfig.setAllowCreate(true);
		
		String indexName = "_ind_".concat(relName).concat(".").concat(column);
		
		// Create the secondary database
		if(getIndexHandle(primaryDb, indexName, mySecConfig) != null)
			created = true;
		
		return created;
	}
	
	
	public Map getSchemaMap()
	{
		return schemaMap;
	}
		
	private void loadSchemaAndIndex()
	{
		Database metaDb;
		Cursor cursor = null;
		boolean indexesLoaded = false;
		boolean indexExists = false;
		
		// Read from metaTable and load each record to the map
		metaDb = getDbHandle("metaTable");
		StoredClassCatalog myClassCatalog = classCatalog;
		
		try {
		cursor = metaDb.openCursor(null, null);
		
		// Create the "data" binding
		EntryBinding dataBinding = new SerialBinding(myClassCatalog,
				GenericTuple.class);
		GenericTuple retTuple = new GenericTuple();
		
		// Get the DatabaseEntry objects that the cursor will use.
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		
		while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
			OperationStatus.SUCCESS) {

			String keyString = new String(foundKey.getData(), "UTF-8");
			retTuple = (GenericTuple) dataBinding.entryToObject(foundData);
			Vector<String> recordValues = new Vector<String>();
			recordValues = (Vector) retTuple.getTuple().clone();
			
			// Add to memory schemaTable
			schemaMap.put(keyString, recordValues);
			
			// Starting from 3, as the first item is the key, for primaryDB
			// and that is already indexed.
			for(int attrPos=3; attrPos<recordValues.size(); attrPos+=3)
			{
				// If attribute is indexed
				if(recordValues.elementAt(attrPos+2).equals("1"))
				{
					String attr = recordValues.elementAt(attrPos).substring(recordValues.elementAt(attrPos).indexOf(".")+1, recordValues.elementAt(attrPos).length());				
					indexExists = true;
					indexesLoaded = createSecondaryDb(keyString, attr, attrPos/3);
				}
				
			}		
			
			}
		
		cursor.close();
		
		if(indexExists && !indexesLoaded)
		{
			System.out.println("ERROR: Unable to load all indexes to memmory");
			return;
		}
		
		} catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		
		
	}
	
	public Transaction getActiveTransaction()
	{
		if (txn == null) {
			
			try {
				txn = myEnv.beginTransaction(null, null);
				txn.setLockTimeout(0);
			} catch (DatabaseException e) {
				e.printStackTrace();
			}
		}

		return txn;
	}
	
	public Transaction getTransaction()
	{
		return txn;
	}
	
	public void invalidateActiveTransaction()
	{
		txn = null;
	}
	
	public SecondaryDatabase getIndexHandle(String indexName)
	{
		SecondaryDatabase secondaryDb = null;
		
		if(activeIndexMap.containsKey(indexName))
		{
			// Return existing index object from Hash-map
			indexHandle = (Index)activeIndexMap.get(indexName);
			secondaryDb = indexHandle.getIndex();
		}		
		
		return secondaryDb;
	}
	
	public SecondaryDatabase getIndexHandle(Database primaryDb, String indexName, SecondaryConfig mySecConfig)
	{
		SecondaryDatabase secondaryDb = null;
		
		if(activeIndexMap.containsKey(indexName))
		{
			// Return existing index object from Hash-map
			indexHandle = (Index)activeIndexMap.get(indexName);
			secondaryDb = indexHandle.getIndex();
		}
		else
		{
			// Create a new index Object
		    activeIndexMap.put(indexName, new Index(primaryDb, indexName, mySecConfig));
			indexHandle = (Index) activeIndexMap.get(indexName);
			secondaryDb = indexHandle.getIndex();
			
		}	
		
		return secondaryDb;
	}
	
	public Database getDbHandle(String dbName)
	{
		Database workDb = null;
		
		if(activeDbMap.containsKey(dbName))
		{
			// Return existing DB object from Hash-map
			dbHandle = (Db)activeDbMap.get(dbName);
			workDb = dbHandle.getDb();
		}
		else
		{
			// Create a new DB Object
		    activeDbMap.put(dbName, new Db(dbName));
			dbHandle = (Db) activeDbMap.get(dbName);
			workDb = dbHandle.getDb();
			
		}	   
	    
	    return workDb;
	    
	}
	


	/**
	* Returns a reference to the single instance.
	*/
	public static DBAccess getInstance() {
		if(DBInstance == null) 
          DBInstance = new DBAccess();
		return DBInstance;
	}
	
	/**
	* The Singleton Constructor.
	*/
	private DBAccess() {}
	
	/*
	 * Starts environment in the specified path (dbFile) and using the
	 * specified settings (readOnly). 
	 */
	public void connect(String dbFile, boolean readOnly) {
		
		if(myEnv != null)
		{
			System.out.println("An environment is already open");
		}
		else
		{

		File envHome = new File(dbFile);

		// Configure environment
		EnvironmentConfig myEnvConfig = new EnvironmentConfig();
		myEnvConfig.setReadOnly(readOnly);
		myEnvConfig.setAllowCreate(!readOnly);
	    myEnvConfig.setTransactional(true);
	    myEnvConfig.setLockTimeout(0);

		// Open the environment
		try {
			myEnv = new Environment(envHome, myEnvConfig);
			
			// Open "metaTable" table
			getDbHandle("metaTable");
			
			// Open the class catalog db (used for class serialization)
			classCatalogDb = getDbHandle("ClassCatalogDB");

			// Create our class catalog
			classCatalog = new StoredClassCatalog(classCatalogDb);
			
		} catch (EnvironmentLockedException e) {
			System.err.println("Error opening environment");
			e.printStackTrace();
		} catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();
		}
		    // Load metadata table and indexes
		    loadSchemaAndIndex();
		
			System.out.println("Environment opened successfully");
		}
	}

	/*
	 * Retrieves the "data" portion of a record given a "key" and table name. 
	 */
	public GenericTuple retrieve(String key, Database dbHandle, Transaction trans, LockMode typeOfLock) {

		GenericTuple tuple = null;
		OperationStatus operStatus = null;

		try {
			
			// Instantiate the class catalog
			StoredClassCatalog classCatalog = new StoredClassCatalog(classCatalogDb);

			// Create the "data" binding
			EntryBinding dataBinding = new SerialBinding(classCatalog,
					GenericTuple.class);

			// Create DatabaseEntry objects for the key and data
			DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry();

			// Get the record
			operStatus = dbHandle.get(trans, theKey, theData, typeOfLock);

			if (operStatus == OperationStatus.SUCCESS)
			{			
			// Recreate the GenericTuple object
			tuple = (GenericTuple) dataBinding.entryToObject(theData);
			}
			
		} catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();
		}  catch (UnsupportedEncodingException e1) {			
			e1.printStackTrace();
		}
		return tuple;
	}

	/*
	 * Retrieves the "data" portion of a record given a "key" and table name
	 * Insert a GenericTuple object as the "data" portion using a specified "key"
	 * and table name.
	 */
	public OperationStatus store(String key, GenericTuple tuple, Database dbHandle, Transaction trans) {

		OperationStatus operStatus = null;

		// Instantiate the class catalog
		StoredClassCatalog classCatalog;
		try {
			classCatalog = new StoredClassCatalog(classCatalogDb);
			
			// Create the binding
			EntryBinding dataBinding = new SerialBinding(classCatalog,
					GenericTuple.class);

			// Create the DatabaseEntry for the key
			DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));

			// Create the DatabaseEntry for the data.
			DatabaseEntry theData = new DatabaseEntry();
			dataBinding.objectToEntry(tuple, theData);

			// Put data to table
			if(dbHandle.getDatabaseName().startsWith("__TEMP_"))
			{
				operStatus = dbHandle.put(trans, theKey, theData);
			}
			else
			{
				operStatus = dbHandle.putNoOverwrite(trans, theKey, theData);
			}
			
			
		} catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();
		} catch (UnsupportedEncodingException e1) {			
			e1.printStackTrace();
		}
		
		return operStatus;

	}

	/*
	 * Close the database and ensure all changes that were made to the database
	 * are saved.
	*/
	public void shutdown() {
		
		if (myEnv != null) {
			try {
				
				// Close secondary DBs first
				int indexMapSize = activeIndexMap.size();

				Iterator keyValuePairs = activeIndexMap.entrySet().iterator();
				for (int a = 0; a < indexMapSize; a++)
				{
				  Map.Entry entry = (Map.Entry) keyValuePairs.next();
				  Index value = (Index)entry.getValue();
				  value.closeIndex();
				}
				
				
				// Now close primary DBs				
				
				int mapSize = activeDbMap.size();

				Iterator keyValuePairs1 = activeDbMap.entrySet().iterator();
				for (int i = 0; i < mapSize; i++)
				{
				  Map.Entry entry = (Map.Entry) keyValuePairs1.next();
				  Db value = (Db)entry.getValue();
				  value.closeDb();
				}
				
				// Close the environment
				myEnv.close();
				DBInstance = null;

			} catch (DatabaseException dbe) {
				System.err.println("Error closing Environment: " + dbe.toString());
				System.exit(-1);
			}
			
			System.out.println("Environment has been closed successfully");
		}
		else
		{
			System.out.println("ERROR: No open environments to close");			
		}
	}

	/*
	 * Get and instance of the current environment settings
	 */
	public Environment getEnvironment() {
		return myEnv;
	}
	

	/*
	 * Get and instance of the current class catalog
	 */
	public StoredClassCatalog getClassCatalog()
	{
		return 	classCatalog;
	}
	
}