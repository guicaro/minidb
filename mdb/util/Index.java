package util;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;


public class Index {
	
	private SecondaryDatabase indexDb = null;
	
	public Index (Database primaryDb, String indexName, SecondaryConfig mySecConfig){
		
		DBAccess dbInstance = DBAccess.getInstance();
		Environment env = dbInstance.getEnvironment();
		
		try {
			indexDb = env.openSecondaryDatabase(null, indexName, primaryDb, mySecConfig);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}

	}
	
	public SecondaryDatabase getIndex(){
		return indexDb;
	}
	
	public void closeIndex(){
		try {

			indexDb.close();

			} catch (DatabaseException dbe) {
				System.err.println("Error closing Index: "
						+ dbe.toString());
			}
	}

}
