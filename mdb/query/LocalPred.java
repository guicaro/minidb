package query;

public final class LocalPred {
	
	private String attribute;
	private String operator;
	private String value;
	

	public LocalPred(String attr, String oper, String val) {
		attribute = attr;
		operator =oper;
		value = val;
	}

	public String getAttribute() {
		return attribute;
	}


	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}


	public String getOperator() {
		return operator;
	}


	public void setOperator(String operator) {
		this.operator = operator;
	}


	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
