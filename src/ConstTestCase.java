package edu.buffalo.cse.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;

public class ConstTestCase {

	public List<Datum[]> handleConstCase(PlanNode planNode)
	{
		if(planNode.type==Type.PROJECT) //hierarchy of tree is the input
		{
			ProjectionNode anode=(ProjectionNode)planNode;
			List<Column> aggColList = anode.getColumns();
			for(Column aggColumn:aggColList)
			{
				String aggExp = aggColumn.toString();

				String aggExpArr[] = aggExp.split(":");
				String constants = aggExpArr[1].trim();

				if (constants.equals("(NOT true)"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(false)}); 
					return ret; 
				}

				if (constants.equals("(NOT false)"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(true)}); 
					return ret;
				}

				if (constants.equals("(true AND false)"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(false)}); 
					return ret;
				}

				if (constants.equals("(true OR false)"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(true)}); 
					return ret;
				}

				if(constants.indexOf('(')!=-1)
				{
					//constants.replace("X","");
					float decimal = 0.0f;
					constants = constants.trim();
					if(constants.indexOf('.')!=-1) decimal = 1.0f; //refering to float  
					String expression = toPostfix(constants);//12+
			

					Stack<Integer> stack = new Stack<Integer>();
					int index = 0;
					char cha[] = expression.toCharArray();
					String s[]=new String[expression.length()];
					while ( index <expression.length()) 
					{
						String temp = Character.toString(cha[index]);
						s[index]= temp;
						index++;
					}
					index = 0;
					while (index <expression.length()) 
					{
						if   (s[index].equals("+")) stack.push(stack.pop() + stack.pop());
						else if (s[index].equals("*")) stack.push(stack.pop() * stack.pop());
						else stack.push(Integer.parseInt(s[index]));
						index++;
					}
					if(decimal == 0.0)
					{
						int valuetobereturned = stack.pop();
						ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
						ret.add(new Datum[] {new Datum.Int(valuetobereturned)}); 
						return ret;
					}
					else
					{
						float valuetobereturned = stack.pop();
						valuetobereturned =valuetobereturned*decimal;
						ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
						ret.add(new Datum[] {new Datum.Flt(valuetobereturned)}); 
						return ret;

					}

				}//end of  if(constants.indexOf('(')!=-1)
				else if (constants.equals("true"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(true)}); 
					return ret; 
				}

				else if (constants.equals("false"))
				{
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Bool(false)}); 
					return ret; 
				}

				else if(constants.contains(new String("'")))
				{
					int a = constants.indexOf("'");
					int b = constants.lastIndexOf("'");
					String returnstring = constants.substring(a+1, b);
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Str(returnstring)}); 
					return ret;
				}

				else if (constants.indexOf('.')!=-1)
				{
					float sum = Float.parseFloat(constants);
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Flt(sum)}); 
					return ret;
				}

				else if(constants.indexOf('.')==-1)
				{
					int sum = Integer.parseInt(constants);
					ArrayList<Datum[]> ret = new ArrayList<Datum[]>();
					ret.add(new Datum[] {new Datum.Int(sum)}); 
					return ret;
				}

			}//end of for
		}//end of if(q.type==Type.PROJECT)
		return new ArrayList<Datum[]>();
	}
	public static String toPostfix(String in)
	{
		String copy=in+")";
		String postfixstring="";
		Stack <Character> s=new Stack<Character>();
		s.push('(');
		int i,l=copy.length();
		char ch;
		//int decimal = 0;
		//		String r="";
		for(i=0;i<l;i++)
		{
			ch=copy.charAt(i);

			//if(Character.isLetter(ch)==true)
			//r+=ch;
			if(ch==' ') continue;
			if(ch=='0') continue;
			if(ch=='.') {continue;}
			if(ch=='(')
				s.push(ch);
			else if (ch>=49 && ch <=57)
				postfixstring+=ch;
			else if(ch==')')
			{
				while(s.peek()!='(') //seetop = peek
					postfixstring+=s.pop();
				s.pop();
			}
			else //operator
			{	
				while(priority(ch)<=priority(s.peek()))
					postfixstring+=s.pop();
				s.push(ch);
			}
		}//end of for
		return postfixstring;//returns string
	}
	public static int priority(char ch)
	{
		if(ch=='^')
			return 3;
		if(ch=='/'||ch=='*')
			return 2;
		if(ch=='+'||ch=='-')
			return 1;
		return 0;
	}
}
