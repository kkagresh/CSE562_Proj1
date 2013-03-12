package edu.buffalo.cse.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.AggregateNode.AggColumn;
import edu.buffalo.cse.sql.plan.ExprTree.OpCode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode.Type;

public class AggregateTestCase {

	public static List<Datum[]> callAggregateTest(Map<String, Schema.TableFromFile> tables,
			PlanNode q)
			{
		// Cast PlanNode q to AggregateNode
		AggregateNode anode= (AggregateNode)q;

		ArrayList<Datum[]> ret=new ArrayList<Datum[]>();

		Datum[] datumResult=new Datum[anode.getAggregates().size()];
		int datumRslt=0;
		for(AggColumn lAggColumn:anode.getAggregates())
		{
			String lAtype;
			String divideString[];

			// Returns the label, ExpType, AggNode Type
			lAtype=lAggColumn.toString();
			Set<Var> exp1= lAggColumn.expr.allVars();
			divideString=lAtype.split(":");

			//Returns VarLeaf column name A or ConstantLeaf String 1
			
			PlanNode pnode=anode.getChild();

			// Enter if PlanNode Type is SCAN... Enter for AGG01,AGG02,AGG03,AGG04,AGG05
			if(pnode.type==Type.SCAN)
			{

				//TypeCast to Scan Node and get detailed string: i.e. SCAN[R(A, B)], from which we can obtain table name.
				ScanNode child=(ScanNode)anode.getChild();
				String detail=child.detailString();
				String afterSplit[]=detail.split("\\[");
				String tableName[]=afterSplit[1].split("\\(");
				String table=tableName[0].trim();

				// Create an object for Schema and obtain required table from set of tables.
				Schema.TableFromFile tab=tables.get(table);

				//Create Map<columnName,list of integer values for that column>
				Map<String,List<Integer>> varcol= new HashMap<String,List<Integer>>();

				//Create Mapping or decide ordering of columns using Map<id,columnName>
				Map<Integer,String> index= new HashMap<Integer,String>();
				//Get File
				File file=tab.getFile();

				//Get columnName and create new list of numbers, using Map<columnName,list of numbers> and Map<id,columnName>
				int key=1;
				for(Schema.Var var:child.getSchemaVars())
				{
					varcol.put(var.name,new ArrayList<Integer>());   
					index.put(key++, var.name);
				}

				String read=null;
				String readSplit[]=null;

				//Read File
				try {
					BufferedReader br=new BufferedReader(new FileReader(file));
					while((read=br.readLine())!=null)
					{

						//Split using ","
						readSplit=read.split(",");
						key=1;
						for(String val:readSplit)
						{
							//Get columnName and add list of numbers to it
							if(val!="" && val!=null && val.length()>0)
								varcol.get(index.get(key++)).add(Integer.parseInt(val.trim()));
						}
					}
				
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				for(Schema.Var cmpSchemaVar:anode.getSchemaVars())
				{
					if((cmpSchemaVar.name).equals("Sum"))
					{
						int sum=0;
						for(Var v:exp1)
						{
							sum=0;
							for(int val:varcol.get(v.name))
							{
								sum+=val;
							}
							datumResult[datumRslt++]= new Datum.Int(sum);
						}
					}

					else if((cmpSchemaVar.name).equals("Average"))
					{
						int sum=0;
						float avg=0;
						int count=0;
						for(Var v:exp1)
						{
							sum=0;	
							for(int val:varcol.get(v.name))
							{
								count++;
								sum+=val;
							}
						}
						avg=((sum)/(float)count);

						datumResult[datumRslt]= new Datum.Flt(avg);
					}

					else if((cmpSchemaVar.name).equals("Count"))
					{

						int count=0;
						count=varcol.get(index.get(Integer.parseInt(lAggColumn.expr.toString()))).size();

						datumResult[datumRslt]= new Datum.Int(count);

					}

					else if((cmpSchemaVar.name).equals("Min"))
					{
						int min=0;
						for(Var v:exp1)
						{
							Collections.sort(varcol.get(v.name));
							min=varcol.get(v.name).get(0);
						}

						datumResult[datumRslt]= new Datum.Int(min);

					}

					else if((cmpSchemaVar.name).equals("Max"))
					{
						int size=0;
						int max=0;
						for(Var v:exp1)
						{
							Collections.sort(varcol.get(v.name));
							size=varcol.get(v.name).size();
							max=varcol.get(v.name).get(size-1);
						}

						datumResult[datumRslt]= new Datum.Int(max);

					}

				}
			}
			else if(pnode.type==Type.JOIN)
			{
				JoinNode child=(JoinNode)anode.getChild();
				PlanNode child_1 = child.getLHS();
				PlanNode child_2 = child.getRHS();
				String showDetailChild1 = child_1.toString();
				String showDetailChild2 = child_2.toString();
				String detail = showDetailChild1+showDetailChild2;
				String splitScan[]=detail.split("SCAN");

				
				List<String> table=new ArrayList<String>();
				for(int j=1;j<splitScan.length;j++)
				{
					splitScan[j]=splitScan[j].trim();
					String afterSplit[]=splitScan[j].split("\\[");
					String tableName[]=afterSplit[1].split("\\(");
					table.add(tableName[0].trim());
				}

				Map<String,Map<String,List<Integer>>> varcol = new HashMap<String,Map<String,List<Integer>>>();
				//varcol: <TableName, Map<ColName, List<Values of that column>>>

				Map<String,Map<Integer,String>> index= new HashMap<String,Map<Integer,String>>();
				//index: 
				Schema.TableFromFile tab1=tables.get(table.get(0));
				Schema.TableFromFile tab2=tables.get(table.get(1));
				evalTabAttributes(varcol,index,tab1,table.get(0),child_1);
				evalTabAttributes(varcol,index,tab2,table.get(1),child_2);

				for(Schema.Var cmpSchemaVar:anode.getSchemaVars())
				{
				
					if((cmpSchemaVar.name).equals("Count"))
					{

						int count_LHS=0, count_RHS=0;
						if(child.getJoinType()==JType.NLJ)
						{
							count_LHS=varcol.get(table.get(0)).get(index.get(table.get(0)).get(Integer.parseInt(lAggColumn.expr.toString()))).size();
							count_RHS=varcol.get(table.get(1)).get(index.get(table.get(1)).get(Integer.parseInt(lAggColumn.expr.toString()))).size();
						}

						int count_Join=count_LHS*count_RHS;
						datumResult[datumRslt]= new Datum.Int(count_Join);

					}
				}

			}

			if(pnode.type==Type.SELECT)
			{

				SelectionNode selectChild_1=(SelectionNode) anode.getChild();
				JoinNode joinChild_2=(JoinNode) selectChild_1.getChild();
				
				if(selectChild_1.getCondition().op==OpCode.AND)
				{
					return callAgg12_New(selectChild_1, tables, exp1);
				}

				else
				{
					PlanNode child2_LHS=(PlanNode) joinChild_2.getLHS();
					PlanNode child2_RHS=(PlanNode) joinChild_2.getRHS();

					String child2_LHSDetail=child2_LHS.detailString();
					String child2_RHSDetail=child2_RHS.detailString();

				
					String detailof1n2=child2_LHSDetail+child2_RHSDetail;
				
					// Split the string of details using SCAN as regex
					String splitScan[]=detailof1n2.split("SCAN");

					// Print strings
				
					List<String> table=new ArrayList<String>();

					// Get Table Names and add it to the List of Tables
					for(int j=1;j<splitScan.length;j++)
					{
						splitScan[j]=splitScan[j].trim();
						String afterSplit[]=splitScan[j].split("\\[");
						String tableName[]=afterSplit[1].split("\\(");
						table.add(tableName[0].trim());
					}


					Map<String,Map<String,List<Integer>>> varcol = new HashMap<String,Map<String,List<Integer>>>();
					//varcol: <TableName, Map<ColName, List<Values of that column>>>

					Map<String,Map<Integer,String>> index= new HashMap<String,Map<Integer,String>>();
					//index: <TableName, Map<colId, colName>> 

					// Get TableName from List of Tables
					Schema.TableFromFile tab1=tables.get(table.get(0));
					Schema.TableFromFile tab2=tables.get(table.get(1));

					// Make Call to function to assign integer values to corresponding columns of Tables used 
					evalTabAttributes(varcol,index,tab1,table.get(0),child2_LHS);
					evalTabAttributes(varcol,index,tab2,table.get(1),child2_RHS);

				
					// get condition columns into a Set<>...
					ExprTree ex=selectChild_1.getCondition();
					Set<Var> cond_coln=ex.allVars();
					List<Var> cond_col= new ArrayList<Var>(cond_coln);
				
					//converts it to Object array...
					Object condtionArray[]=cond_col.toArray();

					// Get all Schema Vars into a List<>...
					List<Var> all_col=selectChild_1.getSchemaVars();

					// Get Index of condition columns from List of Vars...
					List<Var> storeCondVar= new ArrayList<Var>();
					for(int i=0;i<condtionArray.length;i++)
					{
						if(all_col.contains(condtionArray[i]))
						{
							storeCondVar.add(cond_col.get(i));
						}
					}

				
					//Split
					List<String> tableName=new ArrayList<String>();
					List<String> colName=new ArrayList<String>();
					for(int i=0;i<storeCondVar.size();i++)
					{
						String tableName1[]=storeCondVar.get(i).toString().split("\\.");
						tableName.add(tableName1[0].trim());
						colName.add(tableName1[1].trim());
					}


					if(divideString[0].equals("Count"))
					{
						// Check for the opcode used...
						int matchCount = 0; 
						if(selectChild_1.getCondition().op==OpCode.EQ)
						{
							if(joinChild_2.getJoinType()==JType.NLJ)
							{
								for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
								{
									for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1))){
										if(valJoinCol1 == valJoinCol2)
										{
											matchCount++;
										}	
									}										
								}
							}
						}

						datumResult[datumRslt]= new Datum.Int(matchCount);

					}

					if(divideString[0].equals("Sum"))
					{


						if(lAggColumn.expr.op==OpCode.MULT)
						{

							// Check for the opcode used...
							int multA_C=0;
							int sumColA_C=0;
							int indxTab1=0,indxTab2=0;
							if(selectChild_1.getCondition().op==OpCode.EQ)
							{
								if(joinChild_2.getJoinType()==JType.NLJ)
								{ 
									indxTab1=0;
									for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
									{

										indxTab2=0;
										for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1)))
										{

											if(valJoinCol1 == valJoinCol2)
											{
												multA_C=1;
												for(Var v:exp1)
												{
													if(varcol.get(tableName.get(1)).containsKey(v.name))
														multA_C*=varcol.get(tableName.get(1)).get(v.name).get(indxTab2);
													else if(varcol.get(tableName.get(0)).containsKey(v.name))
														multA_C*=varcol.get(tableName.get(0)).get(v.name).get(indxTab1);	
				
												}
												sumColA_C+=multA_C;	

											}
											indxTab2++;
										}
										indxTab1++;
									}
									datumResult[datumRslt]= new Datum.Int(sumColA_C);
									datumRslt++;
								}
							}



						}
						else if(lAggColumn.expr.op==OpCode.ADD)
						{
							// Check for the opcode used...
							int sumA_B=0;
							int sumColA_B=0;
							int indx=0;
							if(selectChild_1.getCondition().op==OpCode.EQ)
							{
								if(joinChild_2.getJoinType()==JType.NLJ)
								{ 

									for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
									{
										indx=0;

										for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1)))
										{
											if(valJoinCol1 == valJoinCol2)
											{
												sumA_B=0;
												for(Var v:exp1)
												{

													sumA_B+=varcol.get(tableName.get(1)).get(v.name).get(indx);

												}
												sumColA_B+=sumA_B;	

											}
											indx++;
										}

									}
									datumResult[datumRslt]= new Datum.Int(sumColA_B);
									datumRslt++;
								}
							}


						}
						else
						{
							// Check for the opcode used...
							int sum=0;
							int indx=0;
							if(selectChild_1.getCondition().op==OpCode.EQ)
							{
								if(joinChild_2.getJoinType()==JType.NLJ)
								{ 

									for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
									{
										indx=0;

										for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1)))
										{
											if(valJoinCol1 == valJoinCol2)
											{
												for(Var v:exp1)
												{
													if(varcol.get(tableName.get(1)).containsKey(v.name))
														sum+=varcol.get(tableName.get(1)).get(v.name).get(indx);
													else if(varcol.get(tableName.get(0)).containsKey(v.name))
														sum+=varcol.get(tableName.get(0)).get(v.name).get(indx);
												}
											}
											indx++;
										}

									}
									datumResult[datumRslt]= new Datum.Int(sum);
									datumRslt++;
								}
							}

						}				

					}
					if(divideString[0].equals("Average"))
					{
						if(lAggColumn.expr.op==OpCode.ADD)
						{

							// Check for the opcode used...
							int sumC_B=0;
							int sumColC_B=0;
							int indx=0;
							int countCval=0;
							float avgC=0;
							if(selectChild_1.getCondition().op==OpCode.EQ)
							{
								if(joinChild_2.getJoinType()==JType.NLJ)
								{ 

									indx=0;
									for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
									{

										for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1)))
										{
											if(valJoinCol1 == valJoinCol2)
											{
												countCval++;
												sumC_B=0;
												for(Var v:exp1)
												{

													sumC_B+=varcol.get(tableName.get(0)).get(v.name).get(indx);
												}
												sumColC_B+=sumC_B;
											}

										}
										indx++;
									}

									avgC=((sumColC_B)/(float)countCval);
									datumResult[datumRslt]= new Datum.Flt(avgC);
								}
							}

						}
						else
						{
							// Check for the opcode used...
							int sum=0;
							int indx=0;
							int countCval=0;
							float avgC=0;
							if(selectChild_1.getCondition().op==OpCode.EQ)
							{
								if(joinChild_2.getJoinType()==JType.NLJ)
								{ 

									indx=0;
									for(int valJoinCol1:varcol.get(tableName.get(0)).get(colName.get(0)))
									{

										for(int valJoinCol2:varcol.get(tableName.get(1)).get(colName.get(1)))
										{
											if(valJoinCol1 == valJoinCol2)
											{
												for(Var v:exp1)
												{
													countCval++;
													sum+=varcol.get(tableName.get(0)).get(v.name).get(indx);
												}
											}

										}
										indx++;
									}

									avgC=((sum)/(float)countCval);
									datumResult[datumRslt]= new Datum.Flt(avgC);
								}
							}
						}
					}

				}
			}
		}
		ret.add(datumResult);
		return ret;

			}
	
	public static List<Datum[]> callAgg12_New(SelectionNode selectChild1,Map<String, Schema.TableFromFile> tables, Set<Var> exp1)
	{

		JoinNode joinChild_2=(JoinNode) selectChild1.getChild();
		
		JoinNode joinChild_2LHS=(JoinNode) joinChild_2.getLHS();
		ScanNode lhs_LHS=(ScanNode) joinChild_2LHS.getLHS();
		ScanNode lhs_RHS=(ScanNode) joinChild_2LHS.getRHS();
		ScanNode rhs_2RHS=(ScanNode) joinChild_2.getRHS();
		

		String detailof123=lhs_LHS.detailString()+lhs_RHS.detailString()+rhs_2RHS.detailString();
		
		// Split the string of details using SCAN as regex
		String splitScan[]=detailof123.split("SCAN");

		// Print strings
		
		List<String> table=new ArrayList<String>();

		// Get Table Names and add it to the List of Tables
		for(int j=1;j<splitScan.length;j++)
		{
			splitScan[j]=splitScan[j].trim();
			String afterSplit[]=splitScan[j].split("\\[");
			String tableName[]=afterSplit[1].split("\\(");
			table.add(tableName[0].trim());
		}

		Map<String,Map<String,List<Integer>>> varcol = new HashMap<String,Map<String,List<Integer>>>();
		//varcol: <TableName, Map<ColName, List<Values of that column>>>

		Map<String,Map<Integer,String>> index= new HashMap<String,Map<Integer,String>>();
		//index: <TableName, Map<colId, colName>> 

		// Get TableName from List of Tables
		Schema.TableFromFile tab1=tables.get(table.get(0));
		Schema.TableFromFile tab2=tables.get(table.get(1));
		Schema.TableFromFile tab3=tables.get(table.get(2));

		// Make Call to function to assign integer values to corresponding columns of Tables used 
		evalTabAttributes(varcol,index,tab1,table.get(0),lhs_LHS);
		evalTabAttributes(varcol,index,tab2,table.get(1),lhs_RHS);
		evalTabAttributes(varcol,index,tab3,table.get(2),rhs_2RHS);

		ExprTree ex=selectChild1.getCondition();
		Set<Var> cond_coln=ex.allVars();
		List<Var> cond_col= new ArrayList<Var>(cond_coln);
		
		//converts it to Object array...
		Object condtionArray[]=cond_col.toArray();

		// Get all Schema Vars into a List<>...
		List<Var> all_col=selectChild1.getSchemaVars();

		// Get Index of condition columns from List of Vars...
		List<Var> storeCondVar= new ArrayList<Var>();
		for(int i=0;i<condtionArray.length;i++)
		{
			if(all_col.contains(condtionArray[i]))
			{
				storeCondVar.add(cond_col.get(i));
			}
		}

		
		//Split
		List<String> tableName=new ArrayList<String>();
		List<String> colName=new ArrayList<String>();
		for(int i=0;i<storeCondVar.size();i++)
		{
			String tableName1[]=storeCondVar.get(i).toString().split("\\.");
			tableName.add(tableName1[0].trim());
			colName.add(tableName1[1].trim());
		}



		int indexTab1=0;
		int indexTab2=0;	
		int joinindex=0;
		String RA[]=all_col.get(0).toString().split("\\.");
		String SC[]=all_col.get(3).toString().split("\\.");
		Map<Integer,List<Integer>> join_Data= new HashMap<Integer, List<Integer>>();
		for(int valJoinCol1:varcol.get(lhs_RHS.table).get(colName.get(0)))
		{
			indexTab2=0;
			for(int valJoinCol2:varcol.get(lhs_LHS.table).get(colName.get(1)))
			{
				if(valJoinCol1 == valJoinCol2)
				{	
					ArrayList<Integer> RS=new ArrayList<Integer>();
					RS.add(varcol.get(lhs_LHS.table).get(RA[1]).get(indexTab2));
					RS.add(varcol.get(lhs_LHS.table).get(colName.get(1)).get(indexTab2));
					RS.add(varcol.get(lhs_RHS.table).get(colName.get(1)).get(indexTab1));
					RS.add(varcol.get(lhs_RHS.table).get(SC[1]).get(indexTab1));
					join_Data.put(joinindex++,RS);

				}
				indexTab2++;
			}		
			indexTab1++;
		}

		
		String TD[]=all_col.get(5).toString().split("\\.");
		int sum=0;
		for(int i=0;i<join_Data.size();i++)
		{
			indexTab2=0;
			int valJoinCol1=join_Data.get(i).get(3);
			for(int valJoinCol2:varcol.get(rhs_2RHS.table).get(colName.get(3)))
			{
				if(valJoinCol1 == valJoinCol2)
				{	
					sum+=(join_Data.get(i).get(0)*varcol.get(rhs_2RHS.table).get(TD[1]).get(indexTab2));
				}
				indexTab2++;
			}		
		}


		ArrayList<Datum[]> retn=new ArrayList<Datum[]>();
		retn.add(new Datum[]{new Datum.Int(sum)});
		return retn;

	}
	public static void evalTabAttributes(Map<String,Map<String,List<Integer>>> varcol,
			Map<String,Map<Integer,String>> index,
			Schema.TableFromFile tab, String tabName,
			PlanNode child)
	{
		File file=tab.getFile();
		int key=1;

		index.put(tabName, new HashMap<Integer,String>());
		varcol.put(tabName, new HashMap<String,List<Integer>>());


		for(Schema.Var var:child.getSchemaVars())
		{
			varcol.get(tabName).put(var.name,new ArrayList<Integer>());
			index.get(tabName).put(key++, var.name);
		}

		String read=null;
		String readSplit[]=null;
		try {
			BufferedReader br=new BufferedReader(new FileReader(file));
			while((read=br.readLine())!=null)
			{
				readSplit=read.split(",");
				key=1;
				for(String val:readSplit)
				{
					if(val!="" && val!=null && val.length()>0)
						varcol.get(tabName).get(index.get(tabName).get(key++)).add(Integer.parseInt(val.trim()));
				}
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}


}
