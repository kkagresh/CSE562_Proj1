package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.buffalo.cse.sql.plan.PlanNode;

public class TableQueryDetails {
public Map<String,Schema.TableFromFile> table=new HashMap<String, Schema.TableFromFile>();
public List<PlanNode> q=new ArrayList<PlanNode>();

public TableQueryDetails() {
	super();
}
public Map<String, Schema.TableFromFile> getTable() {
	return table;
}
public void setTable(Map<String, Schema.TableFromFile> table) {
	this.table = table;
}
public List<PlanNode> getQ() {
	return q;
}
public void setQ(List<PlanNode> q) {
	this.q = q;
}
}
