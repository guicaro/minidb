package util;

import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Database;
import com.sleepycat.je.Transaction;

public interface DBUtil {
	
	/*
	 * Initialize Database
	 */
	public void connect(String dbFile, boolean overwrite);
	
	/*
	 * Put a record to the database.
	 */
	public OperationStatus store(String key, GenericTuple tuple, Database dbHandle, Transaction trans);
	
	/*
	 * Retrieve a serializable class from DB
	 */
	public GenericTuple retrieve(String key, Database dbHandle, Transaction trans, LockMode typeOfLock);

	/*
	 * Close the database and ensure all changes that were made to the database
	 * are saved.
	*/
	public void shutdown();
}

