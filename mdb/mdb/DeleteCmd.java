// Automatically generated code.  Edit at your own risk!
// Generated by bali2jak v2002.09.03.

package mdb;

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

public class DeleteCmd extends Delete {

	final public static int ARG_LENGTH = 2;
	final public static int TOK_LENGTH = 3;
	private Transaction txn;
	DBAccess dbInstance = DBAccess.getInstance();

	public void execute() {

		if (dbInstance.getEnvironment() == null) {
			System.out.println("ERROR: An environment has not been opened");
		} else {
			String relName = getRel_name().toString().trim().toLowerCase();

			// Search every record in database, apply local predicates and then
			// delete qualified records
			applyLocalPredicatesAndDelete(relName);

		}

		super.execute();
	}

	public AstToken getDELETE() {

		return (AstToken) tok[0];
	}

	public One_rel_pred getOne_rel_pred() {

		return (One_rel_pred) arg[1];
	}

	public Rel_name getRel_name() {

		return (Rel_name) arg[0];
	}

	public AstToken getSEMI() {

		return (AstToken) tok[2];
	}

	public AstToken getWHERE() {

		return (AstToken) tok[1];
	}

	public boolean[] printorder() {

		return new boolean[] { true, false, true, false, true };
	}

	public DeleteCmd setParms(AstToken tok0, Rel_name arg0, AstToken tok1,
			One_rel_pred arg1, AstToken tok2) {

		arg = new AstNode[ARG_LENGTH];
		tok = new AstTokenInterface[TOK_LENGTH];

		tok[0] = tok0; /* DELETE */
		arg[0] = arg0; /* Rel_name */
		tok[1] = tok1; /* WHERE */
		arg[1] = arg1; /* One_rel_pred */
		tok[2] = tok2; /* SEMI */

		InitChildren();
		return (DeleteCmd) this;
	}

	public void applyLocalPredicatesAndDelete(String relationName) {
		boolean qualifiedTuple;
		AstCursor c = new AstCursor();
		GenericTuple schemaTuple = new GenericTuple();
		Database workDb = null;
				Cursor cursor = null;

		txn = dbInstance.getActiveTransaction();
		// Open Databases
		workDb = dbInstance.getDbHandle(relationName);
		Map<String, Vector<String>> schemaMap =  dbInstance.getSchemaMap();


		// Get the class catalog, used for data binding.
		StoredClassCatalog myClassCatalog = dbInstance.getClassCatalog();
		
		Vector<String> columnInfo = new Vector<String>();

		// Get the schema information for the relation
		if (schemaMap.containsKey(relationName)) {
			columnInfo = (Vector) schemaMap.get(relationName);
		}
			else {
			System.out.println("ERROR: Table " + relationName
					+ " does not exist");
		}
		

		try {

			c.First(getOne_rel_pred());	
			boolean indexscan=true;
						
			cursor = workDb.openCursor(txn, null);

			// Create the "data" binding
			EntryBinding dataBinding = new SerialBinding(myClassCatalog,
					GenericTuple.class);
			GenericTuple retTuple = new GenericTuple();

			// Get the DatabaseEntry objects that the cursor will use.
			DatabaseEntry foundKey = new DatabaseEntry();
			DatabaseEntry foundData = new DatabaseEntry();
			boolean recordsdeleted = false;
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {

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
							
							int pos=0;

							// Process comparisons on predicates for Integer
							// values
							for (int x = 0; x < recordValues.size(); x ++)

							{

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
							
							int pos=0;

							// Process comparisons on String values
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

				// We now have a qualified tuple, now we want to delete this record
				if (qualifiedTuple) {

					try {
						String keyString = new String(foundKey.getData(),
								"UTF-8");
						System.out.print('\n' + "Deleted : Record : "
								);
						for (int r = 0; r < recordValues.size(); r ++) {
							System.out.print(recordValues.elementAt(r) + " ");

						}

						cursor.delete();
						recordsdeleted = true;

					}

					catch (Exception e) {
						System.out.print(e);

					}

				}

			}

			if (recordsdeleted == false) {
				System.out.print("There are no records to delete");
			}

			cursor.close();

		}

		catch (DatabaseException e) {
			System.err.println("Error opening database: " + e.toString());
			e.printStackTrace();

		}
	}
}
