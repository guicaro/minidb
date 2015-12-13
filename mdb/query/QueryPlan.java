package query;

import java.util.Vector;

public final class QueryPlan {
	
	private int id;
	private int cost;
	private Vector<Relation> relations;
	private Vector<JoinPred> joinPreds;
	private Vector<String> projectedAttr;

	public QueryPlan(int queryPlanId ) {
		
		id = queryPlanId;
		relations = new Vector <Relation>();
		joinPreds = new Vector <JoinPred>();
		projectedAttr = new Vector <String> ();
	}
	
	public QueryPlan(int queryPlanId, Vector<Relation> rels, Vector<JoinPred> joins, Vector<String> projected) {
		
		id = queryPlanId;
		relations = new Vector <Relation>();
		relations.addAll(rels);
		
		joinPreds = joins;
		projectedAttr = projected;
		
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getCost() {
		return cost;
	}

	public void setCost(int cost) {
		this.cost = cost;
	}

	public Vector<Relation> getRelations() {
		return relations;
	}

	public void addRelation(String relName, Vector<String> localPredicates ) {
		
		this.relations.addElement(new Relation(relName, localPredicates));
	}
	
	public void addRelation(String relName) {
		this.relations.addElement(new Relation(relName));
	}
	

	public Vector<JoinPred> getJoinPreds() {
		return joinPreds;
	}

	public void addJoinPred(String attr1, String attr2) {
		this.joinPreds.addElement(new JoinPred(attr1, attr2));
	}

	public Vector<String> getProjectedAttr() {
		return projectedAttr;
	}

	public void addProjectedAttr(String projectedAttr) {
		this.projectedAttr.addElement(projectedAttr);
	}

}
