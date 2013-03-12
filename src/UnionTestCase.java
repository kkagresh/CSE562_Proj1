package edu.buffalo.cse.sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.UnionNode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;

public class UnionTestCase {
	public static List<Datum[]> processUnionQuery(PlanNode q, Map<String,Schema.TableFromFile> tables)
	{
		ArrayList<Datum[]> ret = new ArrayList<Datum[]>();

		if(q.type == Type.UNION)
		{
			UnionNode unode=(UnionNode)q;
			List<Datum[]> returnLhs = processUnionQuery(unode.getLHS(), tables);
			List<Datum[]> returnRhs = processUnionQuery(unode.getRHS(), tables);
			ret.addAll(returnLhs);
			ret.addAll(returnRhs);
			return ret;
		}
		else if(q.type == Type.PROJECT)
		{

			ProjectionNode pnode=(ProjectionNode)q;
			if(pnode.getChild().type==PlanNode.Type.NULLSOURCE)
			{
				NullSourceNode nsnode = (NullSourceNode)pnode.getChild(); 
				List<Column> colList = pnode.getColumns();
				for(int i=0;i<nsnode.rows;i++)
				{
					Datum[] datum = new Datum[colList.size()];
					int index=0;
					for(Column c:colList){
						datum[index++] = new Datum.Int(Integer.parseInt(c.expr.toString()));
					}
					ret.add(datum);
				}
			}
			else if(pnode.getChild().type==PlanNode.Type.SCAN)
			{
				ScanNode snode = (ScanNode)pnode.getChild();
				String table = snode.table;
				Schema.TableFromFile tableData = tables.get(table);

				Map<String,List<Integer>> tableDataMap = new HashMap<String,List<Integer>>();
				//tableDataMap: Map<ColumnName, List<Values>>
				Map<Integer,String> indexColumn = new HashMap<Integer,String>();
				//indexColumn: Map<Index, ColumnName>
				ArrayList<Schema.Var> colList = (ArrayList<Schema.Var>)tableData.names();

				int index=1;
				for(Schema.Var var:colList)
				{
					indexColumn.put(index++,var.name);
					tableDataMap.put(var.name, new ArrayList<Integer>());
				}

				int row=0;
				try {
					BufferedReader br = new BufferedReader(new FileReader(tableData.getFile()));
					String line="";

					while((line=br.readLine())!=null)
					{
						if(line!="" && line.length()>0)
						{
							String lineDetails[]= line.split(",");
							index=1;
							for(String val:lineDetails)
							{
								val = val.trim();
								tableDataMap.get(indexColumn.get(index++)).add(Integer.parseInt(val));
							}
							row++;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
				}

				List<Column> colProjList = pnode.getColumns();
				for(int i=0;i<row;i++)
				{
					Datum[] datum=new Datum[colProjList.size()];
					index=0;
					for(Column cProj:colProjList)
					{
						datum[index++]=new Datum.Int(tableDataMap.get(cProj.name).get(i));
					}
					ret.add(datum);
				}
			}
		}
		return ret;
	}
}
