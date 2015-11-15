package sqlEngine;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import storageManager.*;

class ReadFile
{
	String fileName;
	ArrayList<String> fileData;
	public ReadFile(String var_filename)
	{
		fileName = var_filename;
	}
	
	public ArrayList<String> readFileText()
	{
		BufferedReader in = null;
		try{
			in = new BufferedReader(
					new FileReader(fileName));
			fileData = new ArrayList<String>();
			String str;
			while ((str = in.readLine()) != null)
			{
				fileData.add(str);				
			}
			in.close();			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return fileData;
		
	}
}
class Statement
{
	MainMemory mem;
    Disk disk;   
    SchemaManager schema_manager;
    
	String stmt;
	
	public Statement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		stmt = stmt_var;
		mem = mem_var;
		disk = disk_var;
	    schema_manager =schema_manager_var;
	}
	
	public void analyzeStatement()
	{
		String[] stmtSplit = stmt.split(" ");
		System.out.println(" The statment is : " + stmt);
		// will optimize it later -- Need to remove split 
	
		if(stmtSplit[0].equals("CREATE")){
			CreateStatement cs = new CreateStatement(stmt,mem,disk,schema_manager);
			cs.runStatement();
		}
		else if(stmtSplit[0].equals("INSERT"))
		{
			InsertStatement insertStmt = new InsertStatement(stmt,mem,disk,schema_manager);
			insertStmt.runStatement();
			//This is also done.
		}
		else if(stmtSplit[0].equals("DROP"))
		{
			DropStatement dropStmt = new DropStatement(stmt,mem,disk,schema_manager);
			dropStmt.runStatement();
		}
		else if(stmtSplit[0].equals("SELECT"))
		{
			SelectStatement selectStmt = new SelectStatement(stmt,mem,disk,schema_manager);
			selectStmt.runStatement();
		}
		else if(stmtSplit[0].equals("DELETE"))
		{
			DeleteStatement deleteStmt = new DeleteStatement(stmt,mem,disk,schema_manager);
			deleteStmt.runStatement();
		}
		
	}
}

class DeleteStatement extends Statement
{
	//Inherited member variables are in comments below
	//MainMemory mem;
	//Disk disk;   
	//SchemaManager schema_manager;
	//String stmt;
		
	Relation relation_reference;
	String relation_name;
	
	public DeleteStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public void runStatement()
	{
		String pattern1 = "FROM";
		String pattern2 = "WHERE";
		String whereCondition = "";
		ArrayList<String> tableNames = new ArrayList<String>();
		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		String allTables = "";
		while(m.find())
		{
			allTables = m.group(1);				
		}
		
		String[] allTablesSplit = allTables.split(",");
		for(int i=0; i<allTablesSplit.length; i++)
		{
			tableNames.add(allTablesSplit[i].trim());
		}
		
		//now getting the condition from where clause
		p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
		m = p.matcher(stmt);			
		if ( m.find() ) {
			whereCondition = m.group(1);
		}
		System.out.println(" Where condition is " + whereCondition);
		
		
	}
}

class SelectStatement extends Statement
{
	//Inherited member variables are in comments below
	//MainMemory mem;
    //Disk disk;   
    //SchemaManager schema_manager;
    //String stmt;
	
	
	Relation relation_reference;
	String relation_name;
	
	public SelectStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public boolean runStatement()
	{
		//parsing logic starts here
		String pattern1 = "SELECT";
		String pattern2 = "FROM";
		
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> tableNames = new ArrayList<String>();
		
		String whereCondition = "";

		Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
		Matcher m = p.matcher(stmt);
		while (m.find()) {
			String col = m.group(1);
			columnNames.add(col);		  
		}
		
		if(columnNames.size() == 1 && columnNames.get(0).equals("*"))
		{
			System.out.println("Print all columns");
		}
		
		//getting all table names now
		pattern1 = "FROM";
		pattern2 = "WHERE";
		// for statements like select something/* from tablname where somecondition
		if(stmt.contains("WHERE"))
		{
			p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
			m = p.matcher(stmt);
			String allTables = "";
			while(m.find())
			{
				allTables = m.group(1);				
			}
			
			String[] allTablesSplit = allTables.split(",");
			for(int i=0; i<allTablesSplit.length; i++)
			{
				tableNames.add(allTablesSplit[i].trim());
			}
			
			//now getting the condition from where clause
			p = Pattern.compile(Pattern.quote(pattern2) + "(.*)");
			m = p.matcher(stmt);			
			if ( m.find() ) {
				whereCondition = m.group(1);
			}
			System.out.println(" Where condition is " + whereCondition);
		}
		else
		{
			//this is for statements like select something/*  from tablename ... no condition   "sentence(.*)"
			p = Pattern.compile(Pattern.quote(pattern1) + "(.*)");
			m = p.matcher(stmt);
			String allTables = "";
			if ( m.find() ) {
			   allTables = m.group(1);
			}
			String[] allTablesSplit = allTables.split(",");
			for(int i=0; i<allTablesSplit.length; i++)
			{
				tableNames.add(allTablesSplit[i].trim());
			}			
		}
		
		System.out.println(" Table name is " + tableNames.get(0));
		
		//parsing logic ends here
		
		//following code is assumed for only one table name in from.. Later need to expand it to join 
		relation_reference = schema_manager.getRelation(tableNames.get(0));
		
		//was getting everything in once
		//relation_reference.getBlocks(0,3,relation_reference.getNumOfBlocks());
	    
		
		System.out.println( " Column names " + relation_reference.getSchema().getFieldNames());
		ArrayList<String> fieldNames = relation_reference.getSchema().getFieldNames();
		for( int i=0; i<fieldNames.size(); i++)
		{
			System.out.print(fieldNames.get(i));
			System.out.print("   |  ");
		}
		System.out.println(" ");
		System.out.println("----------------------------------------------------");
		
		
		for(int i=0; i<relation_reference.getNumOfBlocks(); i++)
		{			
			relation_reference.getBlock(i,3);
			
			Block block_reference=mem.getBlock(3);
			Tuple current = block_reference.getTuple(0);
			
			if(testCondition(current,whereCondition) == true)
				System.out.println(current);			
			
			
		}
		
		
	    
		return true;
	}
	private boolean testCondition(Tuple current, String whereClause)
	{
		
		ArrayList<String> postFix = createPostFix(whereClause);		
		
		
		if(postFix.size() == 0)
			return true;
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",2);
		opMap.put("=",1);
		opMap.put("AND",1);
		opMap.put("OR",1);
		opMap.put(">", 1);
		
		Stack<String> output = new Stack<String>();
		
		for(int i=0; i<postFix.size(); i++)
		{
			if(opMap.containsKey(postFix.get(i)))
			{
				String field1 = output.pop();
				String field2 = output.pop();
				
				if(postFix.get(i).equals("+"))
				{
					Integer result = current.getField(field1).integer + current.getField(field2).integer;
					output.push(result.toString());
				}
				else if(postFix.get(i).equals("="))
				{
					if( current.getSchema().fieldNameExists(field2) )
					{
						if(current.getField(field2).type == FieldType.INT)
						{
							output.push(new Boolean(current.getField(field2).integer == Integer.parseInt(field1)).toString());
						}
						
					}
					
				}			
			}
			else
				output.push(postFix.get(i));
		}
		
		return new Boolean(output.pop());
		
	}
	private ArrayList<String> createPostFix(String whereClause)
	{
		String[] tokens = whereClause.split(" ");
		Stack<String> opStack = new Stack<String>();
		
		ArrayList<String> postFix = new ArrayList<String>();
		
		HashMap<String, Integer> opMap = new HashMap<String,Integer>();
		opMap.put("+", 2);
		opMap.put("/", 2);
		opMap.put("*", 2);
		opMap.put("-",1);
		opMap.put("=",1);
		opMap.put("AND",1);
		opMap.put("OR",1);
		opMap.put(">", 1);
		
		for(int i=0; i<tokens.length; i++)
		{
			if((opMap.containsKey(tokens[i]) == false) && !(tokens[i].equals("[")) && !(tokens[i].equals("]")) )
			{
				postFix.add(tokens[i]);
			}
			else if(tokens[i].equals("["))
			{
				opStack.push(tokens[i]);
			}
			else if(opMap.containsKey(tokens[i]) == true)
			{
				while((opStack.size() > 0 ) && !(opStack.peek().equals("[")))
				{
					String topOp = opStack.peek();
					if( opMap.get(topOp) > opMap.get(tokens[i]))
					{
						postFix.add(opStack.pop());
					}
					else
						break;
				}
				opStack.push(tokens[i]);
			}
			else if(tokens[i].equals("]"))
			{
				while ((opStack.size() > 0) && !(opStack.peek().equals("[")))
	            {
					postFix.add(opStack.pop());
	            }
	            if (opStack.size() > 0)
	                opStack.pop(); // popping out the left brace '('				
			}
		}
		
		while(opStack.size() > 0)
			postFix.add(opStack.pop());		
		
	
		return postFix;
	}
}

class DropStatement extends Statement
{
	Relation relation_reference;
	String relation_name;
	public DropStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	public boolean runStatement()
	{
		relation_name = stmt.split(" ")[2];
		System.out.println("Schema manager before deletion " + schema_manager);
		
		if(relation_name != null)
			schema_manager.deleteRelation(relation_name);
		
		System.out.println(" Successfull drop of the table ");
		System.out.println("Schema manager after deletion " + schema_manager);
		

		return true;
	}
}

class InsertStatement extends Statement
{
	Relation relation_reference;
	String relation_name;
	public InsertStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		super(stmt_var,mem_var,disk_var,schema_manager_var);
	}
	
	//Insert Statement
	public boolean runStatement()
	{
		
		String[] attrNames = null; // this will hold attribute names sid, homework etc
		String[] attrValues = null; // this will hold attribute values;
		relation_name = stmt.split(" ")[2];
		relation_reference = schema_manager.getRelation(relation_name);
		// INSERT INTO course (sid, homework, project, exam, grade) VALUES (1, 99, 100, 100, "A")
		
		//parsing logic starts
		stmt = stmt.replace('(', '{');
		stmt = stmt.replace(')', '}');
		
		List<String> allMatches = new ArrayList<String>();
		 Matcher m = Pattern.compile("\\{([^}]*)\\}")
		     .matcher(stmt);
		 while (m.find()) {
		   String matchedPart = m.group();
		   matchedPart= matchedPart.substring(1, matchedPart.length()-1);
		   allMatches.add(matchedPart);		   
		 }	
		
		attrNames = allMatches.get(0).split(",");
		attrValues = allMatches.get(1).split(",");		
		//removing extra spaces if any
		for(int i=0; i<attrNames.length; i++)
		{
			attrNames[i] = attrNames[i].trim();
			attrValues[i] = attrValues[i].trim();
		}		 
		//parsing logic ends 
		
		//now attributes names and values have been collected.Lets create a tuple
		Tuple tuple = relation_reference.createTuple();
		for(int i=0; i<attrNames.length; i++)
		{
			try
			{
				int val = Integer.parseInt(attrValues[i]);
				tuple.setField(attrNames[i], val);
			}
			catch(NumberFormatException ex)
			{
				tuple.setField(attrNames[i], attrValues[i]);
			}
			//tuple.setField(attrNames[i],"v11");
		    
		}
		
		Block block_reference=mem.getBlock(0); //access to memory block 0
	    block_reference.clear(); //clear the block
	    
	    block_reference.appendTuple(tuple);
	    
		
	    Parser.appendTupleToRelation(relation_reference, mem, 9, tuple);	    
	   
		return true;
	}
}

class CreateStatement
{
	MainMemory mem;
    Disk disk;   
    SchemaManager schema_manager;
	
	String stmt;
	ArrayList<String> attr_List;
	ArrayList<FieldType> attr_types;	
	
	public CreateStatement(String stmt_var, MainMemory mem_var,Disk disk_var, SchemaManager schema_manager_var)
	{
		stmt = stmt_var;
		mem = mem_var;
		disk = disk_var;
	    schema_manager =schema_manager_var;
	    
	    attr_List = new ArrayList<String>();
		attr_types = new ArrayList<FieldType>();
	}
	
	public Relation runStatement()
	{
		String tableName = stmt.split(" ")[2];
		getAttributesNameType();
		
		System.out.println(" Statement is  " + stmt);
		
		for(int i=0; i<attr_List.size(); i++)
		{
			System.out.println(" " + attr_List.get(i) + " " + attr_types.get(i));
		}
		Schema schema = new Schema(attr_List,attr_types);
		
		String relation_name=tableName;
	    Relation relation_reference=schema_manager.createRelation(relation_name,schema);		
		
		
		return relation_reference;
	}
	
	private void getAttributesNameType()
	{
		String result = stmt.substring(stmt.indexOf("(") + 1, stmt.indexOf(")"));
		
		String[] resultSplit = result.split(",");
		for(int i=0; i<resultSplit.length; i++)
		{
			String newString = resultSplit[i].trim();
			String[] nameType = newString.split(" ");
			attr_List.add(nameType[0]);
			
			if(nameType[1].equals("INT"))
			{				
				attr_types.add(FieldType.INT);
			}
			else
				attr_types.add(FieldType.STR20);
				
		}
	}
	
	
}

public class Parser {
	
	public static void main(String[] args)
	{
		//ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows - Copy.txt"); //contains file name which contains sql statement
		
		ReadFile rf = new ReadFile("C:/Users/RatneshThakur/Desktop/Course Materials/Database Systems/Database Systems Project 2/TinySQL_windows.txt"); //contains file name which contains sql statement
		
		ArrayList<String> fileData = rf.readFileText();	//contains file data in the form of array list
		
		System.out.println(" Size of the file : " + fileData.size() + " lines");
		Statement st = null;
		
		
		 //=======================Initialization=========================
	    System.out.print("=======================Initialization=========================" + "\n");

	    // Initialize the memory, disk and the schema manager
	    MainMemory mem=new MainMemory();
	    Disk disk=new Disk();
	    //System.out.print("The memory contains " + mem.getMemorySize() + " blocks" + "\n");
	    //System.out.print(mem + "\n" + "\n");
	    SchemaManager schema_manager=new SchemaManager(mem,disk);

	    disk.resetDiskIOs();
	    disk.resetDiskTimer();

		
		for(int i=0; i<fileData.size(); i++)
		{			
			st = new Statement(fileData.get(i), mem,disk, schema_manager);
			st.analyzeStatement();
		}		
	}
	
	public static void appendTupleToRelation(Relation relation_reference, MainMemory mem, int memory_block_index, Tuple tuple) {
	    Block block_reference;
	    if (relation_reference.getNumOfBlocks()==0) {
	      //System.out.print("The relation is empty" + "\n");
	      //System.out.print("Get the handle to the memory block " + memory_block_index + " and clear it" + "\n");
	      block_reference=mem.getBlock(memory_block_index);
	      block_reference.clear(); //clear the block
	      block_reference.appendTuple(tuple); // append the tuple
	      //System.out.print("Write to the first block of the relation" + "\n");
	      relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index);
	    } else {
	      //System.out.print("Read the last block of the relation into memory block 5:" + "\n");
	      relation_reference.getBlock(relation_reference.getNumOfBlocks()-1,memory_block_index);
	      block_reference=mem.getBlock(memory_block_index);

	      if (block_reference.isFull()) {
	        //System.out.print("(The block is full: Clear the memory block and append the tuple)" + "\n");
	        block_reference.clear(); //clear the block
	        block_reference.appendTuple(tuple); // append the tuple
	        //System.out.print("Write to a new block at the end of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks(),memory_block_index); //write back to the relation
	      } else {
	        //System.out.print("(The block is not full: Append it directly)" + "\n");
	        block_reference.appendTuple(tuple); // append the tuple
	        //System.out.print("Write to the last block of the relation" + "\n");
	        relation_reference.setBlock(relation_reference.getNumOfBlocks()-1,memory_block_index); //write back to the relation
	      }
	    }
	  }

}
