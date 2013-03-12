package edu.buffalo.cse.sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;

public class ProjectionTestCase {
	public List<Datum[]> handleProjectionTestCase(Map<String,Schema.TableFromFile> tables,PlanNode planNode)
	{
		ProjectionNode pnode=(ProjectionNode)planNode;
		List<Column> columnList = pnode.getColumns();
		if(pnode.getChild().type==PlanNode.Type.SCAN)
		{
			ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
			String tabName = ((ScanNode)pnode.getChild()).table;

			Map<String,Map<String,List<Integer>>> tableDataMap = new HashMap<String, Map<String,List<Integer>>>();
			//tableDataMap: Map<tab_name, Map <col_name,List<values>>>

			Map<String,Map<Integer,String>> indexCol = new HashMap<String, Map<Integer,String>>();
			//indexCol: Map<tab_name, Map<col_index, col_name>> 

			tableDataMap.put(tabName, new HashMap<String, List<Integer>>());
			indexCol.put(tabName, new HashMap<Integer, String>());
			processScanNode((ScanNode)pnode.getChild(), tableDataMap, indexCol, tables);


			for(String column:tableDataMap.get(tabName).keySet())
			{
				for(Column pnodeCol:columnList){
					if(pnodeCol.name.equals(column)){
						if(pnodeCol.expr.op==OpCode.VAR)
						{
							for(int val:tableDataMap.get(tabName).get(column))
							{
								ret.add(new Datum[]{new Datum.Int(val)});
							}
						}
					}
				}
			}
			return ret;
		}
		else if(pnode.getChild().type==PlanNode.Type.JOIN)
		{
			JoinNode jnode = (JoinNode)pnode.getChild();
			ScanNode snodeLhs = (ScanNode)jnode.getLHS();
			ScanNode snodeRhs = (ScanNode)jnode.getRHS();

			Map<String,Map<String,List<Integer>>> tableDataMap = new HashMap<String, Map<String,List<Integer>>>();
			//tableDataMap: Map<tab_name, Map <col_name,List<values>>>

			Map<String,Map<Integer,String>> indexCol = new HashMap<String, Map<Integer,String>>();
			//indexCol: Map<tab_name, Map<col_index, col_name>> 

			tableDataMap.put(snodeLhs.table, new HashMap<String, List<Integer>>());
			indexCol.put(snodeLhs.table,new HashMap<Integer, String>());

			tableDataMap.put(snodeRhs.table, new HashMap<String, List<Integer>>());
			indexCol.put(snodeRhs.table,new HashMap<Integer, String>());

			int rowJoinTab1 = processScanNode(snodeLhs, tableDataMap, indexCol, tables);
			int rowJoinTab2 = processScanNode(snodeRhs, tableDataMap, indexCol, tables);

			ArrayList<Datum[]> ret = new ArrayList<Datum[]>(); 
			if(jnode.getJoinType()==JoinNode.JType.NLJ)
			{
				for(int i=0;i<rowJoinTab1;i++)
				{
					for(int j=0;j<rowJoinTab2;j++)
					{

						Datum[] datum=new Datum[columnList.size()];
						int index=0;
						for(Column c:columnList)
						{
							String tableExpr = c.expr.toString().split("\\.")[0];
							if(tableExpr.equals(snodeLhs.table) && tableDataMap.get(snodeLhs.table).keySet().contains(c.name))
								datum[index++] = new Datum.Int(tableDataMap.get(snodeLhs.table).get(c.name).get(i));
							else if(tableExpr.equals(snodeRhs.table) && tableDataMap.get(snodeRhs.table).keySet().contains(c.name))
								datum[index++] = new Datum.Int(tableDataMap.get(snodeRhs.table).get(c.name).get(j));
						}
						ret.add(datum);
					}
				}
			}
			return ret;
		}
		else if(pnode.getChild().type==Type.SELECT)
		{
			SelectionNode snode = (SelectionNode)pnode.getChild();

			JoinNode jnode = (JoinNode)snode.getChild();

			if(jnode.getLHS().type==PlanNode.Type.SCAN && jnode.getRHS().type==PlanNode.Type.SCAN)
			{
				ScanNode snodeLhs = (ScanNode)jnode.getLHS();
				ScanNode snodeRhs = (ScanNode)jnode.getRHS();

				Map<String,Map<String,List<Integer>>> tableDataMap = new HashMap<String, Map<String,List<Integer>>>();
				//tableDataMap: Map<tab_name, Map <col_name,List<values>>>

				Map<String,Map<Integer,String>> indexCol = new HashMap<String, Map<Integer,String>>();
				//indexCol: Map<tab_name, Map<col_index, col_name>> 

				tableDataMap.put(snodeLhs.table, new HashMap<String, List<Integer>>());
				indexCol.put(snodeLhs.table,new HashMap<Integer, String>());

				tableDataMap.put(snodeRhs.table, new HashMap<String, List<Integer>>());
				indexCol.put(snodeRhs.table,new HashMap<Integer, String>());


				int rowJoinTab1 = processScanNode(snodeLhs, tableDataMap, indexCol, tables);
				int rowJoinTab2 = processScanNode(snodeRhs, tableDataMap, indexCol, tables);

				ArrayList<Datum[]> ret = new ArrayList<Datum[]>();

				Set<Var> allVars = snode.getCondition().allVars();

				String tablesJoin[] = new String[allVars.size()];
				String columnJoin[] = new String[allVars.size()];
				int index=0;

				for(Var v:allVars)
				{
					tablesJoin[index] = v.toString().split("\\.")[0];
					columnJoin[index++] = v.toString().split("\\.")[1];
				}


				for(int i=0;i<rowJoinTab1;i++)
				{
					for(int j=0;j<rowJoinTab2;j++)
					{
						if(resolveExpression(snode.getCondition(),tableDataMap,snodeLhs.table,snodeRhs.table,i,j))
						{
							Datum[] datum=new Datum[columnList.size()];
							index=0;
							for(Column c:columnList)
							{
								if(tableDataMap.get(snodeLhs.table).keySet().contains(c.name))
									datum[index++] = new Datum.Int(tableDataMap.get(snodeLhs.table).get(c.name).get(i));
								else if(tableDataMap.get(snodeRhs.table).keySet().contains(c.name))
									datum[index++] = new Datum.Int(tableDataMap.get(snodeRhs.table).get(c.name).get(j));
							}
							ret.add(datum);
						}
					}
				}
				return ret;
			}
			else//Either of them is a JoinNode (i.e. Tree of JoinNode and ScanNode)
			{
				JoinNode  jnodeLhs=(JoinNode)jnode.getLHS();
				ScanNode scanNodeRhs = (ScanNode)jnode.getRHS();
				ScanNode jscanNodeLhs = (ScanNode)jnodeLhs.getLHS();
				ScanNode jscanNodeRhs = (ScanNode)jnodeLhs.getRHS();

				Map<String,Map<String,List<Integer>>> tableDataMap = new HashMap<String, Map<String,List<Integer>>>();
				//tableDataMap: Map<tab_name, Map <col_name,List<values>>>

				Map<String,Map<Integer,String>> indexCol = new HashMap<String, Map<Integer,String>>();
				//indexCol: Map<tab_name, Map<col_index, col_name>> 

				tableDataMap.put(scanNodeRhs.table, new HashMap<String, List<Integer>>());
				indexCol.put(scanNodeRhs.table,new HashMap<Integer, String>());

				tableDataMap.put(jscanNodeLhs.table, new HashMap<String, List<Integer>>());
				indexCol.put(jscanNodeLhs.table,new HashMap<Integer, String>());

				tableDataMap.put(jscanNodeRhs.table, new HashMap<String, List<Integer>>());
				indexCol.put(jscanNodeRhs.table,new HashMap<Integer, String>());

				int rowsNRhs = processScanNode(scanNodeRhs, tableDataMap, indexCol, tables);
				int rowjsNLhs = processScanNode(jscanNodeLhs, tableDataMap, indexCol, tables);
				int rowjsNRhs = processScanNode(jscanNodeRhs, tableDataMap, indexCol, tables);

				ExprTree lhsExp = snode.getCondition().get(0);
				ExprTree rhsExp = snode.getCondition().get(1);
				OpCode expOp = snode.getCondition().op;

				Map<Integer,ArrayList<Integer>> interMediateResults = new HashMap<Integer, ArrayList<Integer>>();

				int index=0;
				for(int i=0;i<rowjsNLhs;i++)
				{
					for(int j=0;j<rowjsNRhs;j++)
					{
						if(tableDataMap.get(jscanNodeLhs.table).get(lhsExp.get(0).toString().split("\\.")[1]).get(i) == 
							tableDataMap.get(jscanNodeRhs.table).get(lhsExp.get(1).toString().split("\\.")[1]).get(j))
						{
							ArrayList<Integer> temp=new ArrayList<Integer>();

							temp.add(tableDataMap.get(jscanNodeLhs.table).get(jscanNodeLhs.getSchemaVars().get(0).toString().split("\\.")[1]).get(i));
							temp.add(tableDataMap.get(jscanNodeLhs.table).get(jscanNodeLhs.getSchemaVars().get(1).toString().split("\\.")[1]).get(i));

							temp.add(tableDataMap.get(jscanNodeRhs.table).get(jscanNodeRhs.getSchemaVars().get(0).toString().split("\\.")[1]).get(j));
							temp.add(tableDataMap.get(jscanNodeRhs.table).get(jscanNodeRhs.getSchemaVars().get(1).toString().split("\\.")[1]).get(j));

							interMediateResults.put(index++,temp);
						}
					}
				}

				ArrayList<Datum[]> ret = new ArrayList<Datum[]>();

				for(int i:interMediateResults.keySet())
				{
					for(int j=0;j<rowsNRhs;j++)
					{
						if(rhsExp.op==OpCode.EQ)
						{	
							if(interMediateResults.get(i).get(3) ==
								tableDataMap.get(scanNodeRhs.table).get(rhsExp.get(1).toString().split("\\.")[1]).get(j))
							{
								Datum[] d = new Datum[2];
								d[0] = new Datum.Int(interMediateResults.get(i).get(0));
								d[1] = new Datum.Int(tableDataMap.get(scanNodeRhs.table).get(scanNodeRhs.getSchemaVars().get(1).toString().split("\\.")[1]).get(j));
								ret.add(d);
							}
						}
						else if(rhsExp.op==OpCode.LT)
						{
							if(rhsExp.get(1).toString().split("\\.")[0].equals(scanNodeRhs.table))
							{
								if(interMediateResults.get(i).get(3) <
									tableDataMap.get(scanNodeRhs.table).get(rhsExp.get(1).toString().split("\\.")[1]).get(j))
								{
									Datum[] d = new Datum[2];
									d[0] = new Datum.Int(interMediateResults.get(i).get(0));
									d[1] = new Datum.Int(tableDataMap.get(scanNodeRhs.table).get(scanNodeRhs.getSchemaVars().get(1).toString().split("\\.")[1]).get(j));
									ret.add(d);
								}
							}
							else
							{
								if(interMediateResults.get(i).get(3) >
										tableDataMap.get(scanNodeRhs.table).get(rhsExp.get(1).toString().split("\\.")[1]).get(j))
									{
										Datum[] d = new Datum[2];
										d[0] = new Datum.Int(interMediateResults.get(i).get(0));
										d[1] = new Datum.Int(tableDataMap.get(scanNodeRhs.table).get(scanNodeRhs.getSchemaVars().get(1).toString().split("\\.")[1]).get(j));
										ret.add(d);
									}
							}
						}
					}
				}
				return ret;	
			}
		}
		return new ArrayList<Datum[]>();	
	}
	public static int processScanNode(ScanNode snode,
			Map<String,Map<String,List<Integer>>> tableDataMap,
			Map<String,Map<Integer,String>> indexCol,
			Map<String, Schema.TableFromFile> tables){
		//ScanNode child =(ScanNode)pnode.getChild();
		String tabName = snode.table;
		Schema.TableFromFile tableData = tables.get(tabName);

		int index=1;
		int row=0;
		for(Var col:snode.getSchemaVars())
		{
			tableDataMap.get(tabName).put(col.name, new ArrayList<Integer>());
			indexCol.get(tabName).put(index++,col.name);
		}
		try {
			BufferedReader br=new BufferedReader(new FileReader(tableData.getFile()));
			String line="";

			while((line=br.readLine())!=null)
			{
				if(line!=null && line!="" && line.length()>0)
				{
					String lineDetails[] = line.split(",");
					index=1;
					for(String val:lineDetails)
					{
						tableDataMap.get(tabName).get(indexCol.get(tabName).get(index++)).add(Integer.parseInt(val));
					}
					row++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return row;
	}

	public static boolean resolveExpression(ExprTree e,
			Map<String,Map<String,List<Integer>>> tableDataMap,
			String tab1Name, String tab2Name,
			int tab1Row,int tab2Row)
	{
		if(e.op==OpCode.AND)
		{
			boolean result=true;
			for(ExprTree innerExp:e)
				result= result && resolveExpression(innerExp,tableDataMap,tab1Name,tab2Name,tab1Row,tab2Row);
			return result;
		}
		else if(e.op==OpCode.OR)
		{
			boolean result=false;
			for(ExprTree innerExp:e)
				result= result || resolveExpression(innerExp,tableDataMap,tab1Name,tab2Name,tab1Row,tab2Row);
			return result;
		}
		else{
			Set<Var> vars = e.allVars();
			List<Var> varsList = new ArrayList<Schema.Var>(vars);
			String t1=varsList.get(0).toString().split("\\.")[0];
			String c1=varsList.get(0).toString().split("\\.")[1];
			String t2=varsList.get(1).toString().split("\\.")[0];
			String c2=varsList.get(1).toString().split("\\.")[1];
			if(e.op==OpCode.GT)
			{
				if(t1.equals(tab1Name) && t2.equals(tab2Name))
				{
					if(tableDataMap.get(t1).get(c1).get(tab1Row) > tableDataMap.get(t2).get(c2).get(tab2Row))
						return true;
					else
						return false;
				}
				else if(t2.equals(tab1Name) && t1.equals(tab2Name)){
					if(tableDataMap.get(t1).get(c1).get(tab2Row) > tableDataMap.get(t2).get(c2).get(tab1Row))
						return true;
					else
						return false;
				}
			}
			else if(e.op==OpCode.LT)
			{
				if(t1.equals(tab1Name) && t2.equals(tab2Name))
				{
					if(tableDataMap.get(t1).get(c1).get(tab1Row) < tableDataMap.get(t2).get(c2).get(tab2Row))
						return true;
					else
						return false;
				}
				else if(t2.equals(tab1Name) && t1.equals(tab2Name)){
					if(tableDataMap.get(t1).get(c1).get(tab2Row) < tableDataMap.get(t2).get(c2).get(tab1Row))
						return true;
					else
						return false;
				}
			}
			else if(e.op==OpCode.EQ)
			{
				if(t1.equals(tab1Name) && t2.equals(tab2Name))
				{
					if(tableDataMap.get(t1).get(c1).get(tab1Row) == tableDataMap.get(t2).get(c2).get(tab2Row))
						return true;
					else
						return false;
				}
				else if(t2.equals(tab1Name) && t1.equals(tab2Name)){
					if(tableDataMap.get(t1).get(c1).get(tab2Row) == tableDataMap.get(t2).get(c2).get(tab1Row))
						return true;
					else
						return false;
				}
			}
			else
				return false;
		}
		return false;
	}

}
