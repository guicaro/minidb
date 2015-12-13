package query;

import java.util.Vector;

public final class Relation {

	private String relationName = null;
	private int scanType;  			//1 IndexScan, 0 NormalScan
	private int scanAttrPosition;
	private Vector<LocalPred> localPreds = null;
	
	public Relation(String relName, Vector<String> localPredicates) {
		relationName = relName;
		localPreds = new Vector<LocalPred>();
		
		for(int x=0; x<localPredicates.size(); x+=3)
		{
			if(localPredicates.elementAt(x).startsWith(relName))
			{
				//Initialize with (attribute, operator, value)
				localPreds.addElement(new LocalPred(
                         localPredicates.elementAt(x),
                         localPredicates.elementAt(x+1),
                         localPredicates.elementAt(x+2)));
			}
		}
		
	}

	public Relation (String relName) {
		relationName = relName;
		localPreds = new Vector<LocalPred>();
	}

	public String getRelationName() {
		return relationName;
	}

	public int getScanType() {
		return scanType;
	}

	public void setScanType(int scanType) {
		this.scanType = scanType;
	}

	public int getScanAttrPosition() {
		return scanAttrPosition;
	}

	public void setScanAttrPosition(int scanAttrPosition) {
		this.scanAttrPosition = scanAttrPosition;
	}

	public Vector<LocalPred> getLocalPreds() {
		return localPreds;
	}
	
	public void addLocalPred(String attr, String oper, String val) {
		this.localPreds.addElement(new LocalPred(attr, oper, val));
	} 

}
