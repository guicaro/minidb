package query;

public final class JoinPred {
	
	private String leftAttr;
	private String rightAttr;
	
	public JoinPred(String attr1, String attr2) {
		leftAttr = attr1;
		rightAttr = attr2;
	}

	public String getLeftAttr() {
		return leftAttr;
	}

	public void setLeftAttr(String leftAttr) {
		this.leftAttr = leftAttr;
	}

	public String getRightAttr() {
		return rightAttr;
	}

	public void setRightAttr(String rightAttr) {
		this.rightAttr = rightAttr;
	}
	
}
