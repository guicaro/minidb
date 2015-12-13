// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;
import query.*;

import java.util.*;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import util.DBAccess;
import util.GenericTuple;

public class UpdateCmd extends Update {

	final public static int ARG_LENGTH = 3;
	final public static int TOK_LENGTH = 4;
	private Transaction txn;
	DBAccess dbInstance = DBAccess.getInstance();

	public void execute() {
			    

		// We need an active environment to run operations
		if (dbInstance.getEnvironment() == null) {
			System.out.println("ERROR: An environment has not been opened");
		} else {
			String relName = getRel_name().toString().trim().toLowerCase();

			// Filter by using predicates to select appropiate record to update
			applyLocalPredicatesAndUpdate(relName);
		}

		super.execute();
	}

	public Assign_list getAssign_list() {

		return (Assign_list) arg[1];
	}

	public One_rel_pred getOne_rel_pred() {

		return (One_rel_pred) arg[2];
	}

	public Rel_name getRel_name() {

		return (Rel_name) arg[0];
	}

	public AstToken getSEMI() {

		return (AstToken) tok[3];
	}

	public AstToken getSET() {

		return (AstToken) tok[1];
	}

	public AstToken getUPDATE() {

		return (AstToken) tok[0];
	}

	public AstToken getWHERE() {

		return (AstToken) tok[2];
	}

	public boolean[] printorder() {

		return new boolean[] { true, false, true, false, true, false, true };
	}

	public UpdateCmd setParms(AstToken tok0, Rel_name arg0, AstToken tok1,
			Assign_list arg1, AstToken tok2, One_rel_pred arg2, AstToken tok3) {

		arg = new AstNode[ARG_LENGTH];
		tok = new AstTokenInterface[TOK_LENGTH];

		tok[0] = tok0; /* UPDATE */
		arg[0] = arg0; /* Rel_name */
		tok[1] = tok1; /* SET */
		arg[1] = arg1; /* Assign_list */
		tok[2] = tok2; /* WHERE */
		arg[2] = arg2; /* One_rel_pred */
		tok[3] = tok3; /* SEMI */

		InitChildren();
		return (UpdateCmd) this;
	}
	
	/* 
	 * Will apply local predicates to a database and then update record  
	 */
	public void applyLocalPredicatesAndUpdate(String relationName) {
		boolean qualifiedTuple;
		AstCursor c = new AstCursor();
		GenericTuple schemaTuple = new GenericTuple();
		Database workDb = null;
		Database metaDb = null;
		Cursor cursor = null;

		txn = dbInstance.getActiveTransaction();

		// Open Databases
		workDb = dbInstance.getDbHandle(relationName);
		Map<String, Vector<String>> schemaMap =  dbInstance.getSchemaMap();
		// Get the class catalog, used for data binding.
		StoredClassCatalog myClassCatalog = dbInstance.getClassCatalog();

		// Get the schema information for the relation
		Vector<String> columnInfo = new Vector<String>();

		if (schemaMap.containsKey(relationName)) {
			columnInfo = (Vector) schemaMap.get(relationName);
		}
			else {
			System.out.println("ERROR: Table " + relationName
					+ " does not exist");
		}
		try {

			cursor = workDb.openCursor(txn, null);

			// Create the "data" binding
			EntryBinding dataBinding = new SerialBinding(myClassCatalog,
					GenericTuple.class);
			GenericTuple retTuple = new GenericTuple();

			// Get the DatabaseEntry objects that the cursor will use.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();

			boolean recordsupdated = false;

			while (cursor.getNext(foundKey, foundData,
					LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {

				// Initialize the qualifiedTupleflag and pointer to AST Query
				// tree
				qualifiedTuple = false;
				c.First(getOne_rel_pred());

				// Check if there was a WHERE clause predicate by checking if
				// there
				// are any nodes in the AST Query tree.
				if (!c.More()) {
					// Set the flag for qualifedTupple since there are no local
					// predicates
					// to apply.
					qualifiedTuple = true;
				}

				retTuple = (GenericTuple) dataBinding.entryToObject(foundData);

				// Vector contains data in form: | Data | Type | Data | Type |
				// ...
				Vector<String> recordValues = new Vector<String>();
				recordValues = (Vector) retTuple.getTuple().clone();

				// Process all local predicates in WHERE clause
				while (c.More()) {
					// Set AST Query tree cursor at location to retrieve
					// predicate information
					if (c.node.className().equals("OneRelClause")) {
						c.PlusPlus();
						String predAttr1 = c.node.toString().trim();
						c.PlusPlus();
						String predTest = c.node.className().toString().trim();
						c.PlusPlus();
						int intPredValue;
						String stringPredValue;

						// Check if the data type for the attribute value is an
						// Integer or String
						// Need this check as comparisons (ex. >,<,=,..) are
						// different for each case
						
						if (c.node.className().toString().equalsIgnoreCase(
								"IntLit")) {
							intPredValue = Integer.parseInt(c.node.toString());

							// Process comparisons on predicates for Integer
							// values
							int pos=0;
							for (int x = 0; x < recordValues.size(); x ++) {
								String relName = getRel_name().toString()
										.trim();
								String predAttr = relName + '.' + predAttr1;
								// Use the column information to map to the
								// correct record value
								// (ex. We want to compare an id value of 5
								// against a value of the id
								// column and not age.
								if (columnInfo.elementAt(pos).equalsIgnoreCase(
										predAttr)) {
									if (predTest.equalsIgnoreCase("Equ")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) == intPredValue)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Leq")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) <= intPredValue)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Geq")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) >= intPredValue)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Gtr")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) > intPredValue)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Lss")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) < intPredValue)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Neq")) {
										if (Integer.parseInt(recordValues
												.elementAt(x)) != intPredValue)
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
						} else {
							stringPredValue = c.node.toString().trim();

							// Process comparisons on String values
							int pos=0;
							for (int x = 0; x < recordValues.size(); x ++) {
								String relName = getRel_name().toString()
										.trim();
								String predAttr = relName + '.' + predAttr1;

								if (columnInfo.elementAt(pos).equalsIgnoreCase(
										predAttr)) {
									if (predTest.equalsIgnoreCase("Equ")) {
										if (recordValues.elementAt(x)
												.equalsIgnoreCase(
														stringPredValue))
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Leq")) {
										if (recordValues.elementAt(x)
												.compareToIgnoreCase(
														stringPredValue) < 0
												|| recordValues
														.elementAt(x)
														.compareToIgnoreCase(
																stringPredValue) == 0)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Geq")) {
										if (recordValues.elementAt(x)
												.compareToIgnoreCase(
														stringPredValue) > 0
												|| recordValues
														.elementAt(x)
														.compareToIgnoreCase(
																stringPredValue) == 0)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Gtr")) {
										if (recordValues.elementAt(x)
												.compareToIgnoreCase(
														stringPredValue) > 0)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;

									} else if (predTest.equalsIgnoreCase("Lss")) {
										if (recordValues.elementAt(x)
												.compareToIgnoreCase(
														stringPredValue) < 0)
											qualifiedTuple = true;
										else
											qualifiedTuple = false;
									} else if (predTest.equalsIgnoreCase("Neq")) {
										if (!recordValues.elementAt(x)
												.equalsIgnoreCase(
														stringPredValue))
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
						}
						// Stop processing additional predicates on this record
						if (qualifiedTuple == false) {
							break;
						}
					}

					// Next local predicate
					c.PlusPlus();
				}

				// Check if qualified record, if so, perform update
				if (qualifiedTuple) {

					AstCursor c2 = new AstCursor();
					c2.First(getAssign_list());
					while (c2.More()) {
						if (c2.node.className().toString().equalsIgnoreCase(
								"FieldName")) {
							String predAttr1 = c2.node.toString().trim();
							c2.PlusPlus();
							String relName = getRel_name().toString().trim();
							String predAttr = relName + '.' + predAttr1;

							int pos2=0;
							
							for (int x = 0; x < recordValues.size(); x ++)

							{
								
								if (columnInfo.elementAt(pos2).equalsIgnoreCase(
										predAttr)) {
									String PredValue = c2.node.toString();
									recordValues.set(x, PredValue);

									retTuple.setTuple(recordValues);
									String insertKey = recordValues
											.elementAt(0);
									try {
										cursor.delete();

									}

									catch (Exception e) {
										System.out.print(e);

									}
									OperationStatus insertStatus = dbInstance
											.store(insertKey, retTuple, workDb,
													txn);
									if (insertStatus == OperationStatus.SUCCESS) {
										recordsupdated = true;
										try {

											System.out
													.print('\n'
															+ "Updated :  Key | Data : "
															+ insertKey + " | ");
											for (int r = 0; r < recordValues
													.size(); r ++) {
												System.out.print(recordValues
														.elementAt(r)
														+ " ");

											}
										}

										catch (Exception e) {
											System.out.print(e);

										}

									}

								}
                              pos2=pos2+3;
							}

						}

						c2.PlusPlus();
					}

				}

			}

			if (recordsupdated == false) {
				System.out.print("There are no qualified records to update");
			}

			// Cursors must be closed.
			cursor.close();

		} catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();
		}
	}
}