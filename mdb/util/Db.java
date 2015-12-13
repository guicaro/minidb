package util;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryConfig;


public class Db {
	
	private Database workDb = null;
	
	public Db (String dbName){
		
		DBAccess dbInstance = DBAccess.getInstance();
		Environment env = dbInstance.getEnvironment();
		
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setTransactional(true);
	    dbConfig.setAllowCreate(true);
		dbConfig.setReadOnly(false);
		
		
		// Used as a place holder for joining databases
		if(dbName.startsWith("__TEMP_"))
		{
			dbConfig.setTemporary(true);
		    dbConfig.setTransactional(false);
		    dbConfig.setSortedDuplicates(true);
		}
	    
		try {
			workDb = env.openDatabase(null, dbName, dbConfig);
		} catch (DatabaseException e) {
				System.out.println("Error opening database: "
						+ e.toString());			
		}
	}
	
	public Database getDb(){
		return workDb;
	}
	
	public void closeDb(){
		try {

			workDb.close();

			} catch (DatabaseException dbe) {
				System.out.println("Error closing Database: "
						+ dbe.toString());
			}
	}

}
