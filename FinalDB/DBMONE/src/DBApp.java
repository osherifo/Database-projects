
	import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.File;
	import java.io.FileNotFoundException;
	import java.io.RandomAccessFile;
	import java.io.FileReader;
	import java.io.FileWriter;
	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.Collections;
	import java.util.Comparator;
	import java.util.Enumeration;
	import java.util.Hashtable;
	import java.util.Iterator;
	import java.util.LinkedHashMap;
	import java.util.Properties;
	import java.util.Set;
	import java.util.StringTokenizer;

	import comparators.DBAppComparator;
	import jdbm.*;
	import jdbm.btree.BTree;
	import exceptions.DBAppException;


	public class DBApp {

		int BPlusTreeN, MaximumRowsCountinPage;
		static final String DATABASE = "database";
		RecordManager recmanager;
		/**
		 * Sets the Maximum Rows allowed in one page  
		 *
		 * @param  MR the number of rows to be set
		 */
		public void setMaxRows(int MR) {
			Properties prop = new Properties();
			FileWriter writer = null;

			try {
				writer = new FileWriter("config/DBApp.properties",true);
				prop.setProperty("MaximumRowsCountinPage",
						((Integer) MR).toString());
				prop.store(writer, null);
			} catch (IOException e) {

				e.printStackTrace();
			} finally {
				if (writer != null) {
					try {
						writer.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		/**
		 * Sets BTree maximum elements allowed per node  
		 *
		 * @param  BN the number of elements per node
		 */
		public void setBtreeN(int BN) {
			Properties prop = new Properties();
			FileWriter writer = null;

			try {
				writer = new FileWriter("config/DBApp.properties",true);
				prop.setProperty("BPlusTreeN", ((Integer) BN).toString());
				prop.store(writer, null);
			} catch (IOException e) {

				e.printStackTrace();
			} finally {
				if (writer != null) {
					try {
						writer.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		/**
		 * initializes the BTree N and maximum rows per page  
		 *
		 * 
		 */

		public void loadProperties() {

			Properties prop = new Properties();
			FileReader rdr = null;

			try {
				rdr = new FileReader("config/DBApp.properties");
				prop.load(rdr);
				BPlusTreeN = Integer.parseInt((prop.getProperty("BPlusTreeN")));
				MaximumRowsCountinPage = Integer.parseInt((prop
						.getProperty("MaximumRowsCountinPage")));

			} catch (IOException e) {

				e.printStackTrace();
			} finally {
				if (rdr != null) {
					try {
						rdr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}
		/**
		 * Creates a table given the name and columns 
		 *
		 * @param strTableName The name of the table
		 * @param htblColNameType the types of the columns
		 * @param htblColNameRefs the references of each column
		 * @param strKeyColName the key column
		 * @throws DBAppException
		 */

		public void createTable(String strTableName,
				Hashtable<String, String> htblColNameType,
				Hashtable<String, String> htblColNameRefs, String strKeyColName)
				throws DBAppException {
			if (!tableExists(strTableName)) {
				if (!htblColNameType.containsKey(strKeyColName)) {
					throw new DBAppException(
							"The primary key is not in the columns");
				}
			  Enumeration<String> references = htblColNameRefs.keys();
			  while (references.hasMoreElements()){
				  String refKey =  htblColNameRefs.get(references.nextElement());
				  if(!refKey.equals("null")){
					  StringTokenizer ref = new StringTokenizer(refKey,".");
				String referencedTable = ref.nextToken();
				String referencedColumn = ref.nextToken();
					  if(!tableExists(referencedTable)){
						  throw new DBAppException(
									"The table referenced does not exist");
					  }
					if(!referencedColumn.equals(getPrimaryKey(referencedTable))){
						  throw new DBAppException(
									"The value of the referenced table is not the primary key");
					}
				  }
			  }

				addMetaData(strTableName, htblColNameType, htblColNameRefs,
						strKeyColName);
				CreateFile(strTableName, "1");
				createIndex(strTableName, strKeyColName);
			} else
				throw new DBAppException("table already exists");

		}
		/**
		 * initializes the record manager and the properties 
		 *
		 * 
		 */

		public void init() {
			try {
				recmanager = RecordManagerFactory.createRecordManager(DATABASE);
				loadProperties();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		/**
		 * creates an index on a specified column 
		 *
		 * @param strTableName The name of the table
		 * @param strColName the column to be indexed
		 * @throws DBAppException
		 */

		public void createIndex(String strTableName, String strColName)
				throws DBAppException {
			try {
	 if(!tableExists(strTableName)){
		 throw new DBAppException("no table by the name "+strTableName+" Exists");
	 }
	  int columnCheck = getColumnIndex(strTableName, strColName);
	  if(columnCheck==-1){
		  throw new DBAppException("The column "+strColName+" does not Exist in the table");
	  }
	 
				long recid;
				String treename = strTableName + "," + strColName;
				recid = recmanager.getNamedObject(treename);
				if (recid != 0)
					throw new DBAppException("index already exists");
				else {

					Comparator comp = new DBAppComparator();

					BTree tree = BTree.createInstance(recmanager, comp);

					recmanager.setNamedObject(treename, tree.getRecid());
					// 3ATEF check el awal en mfeesh record asln fel tree bnfs el
					// key law feeh e3ml linkedlist
					insertRecordsinTree(tree, strTableName, strColName);
					if(!getPrimaryKey(strTableName).equals(strColName))
					updateMetaDataIndex(strTableName, strColName);
					recmanager.commit();

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		// 3ATEF
		/**
		 * updates the metadata of the indexed column to be true
		 *
		 * @param table The name of the table
		 * @param col the name of the column
		 */
		public void updateMetaDataIndex(String table, String col) {
			try {
				// RandomAccessFile ram=new
				// RandomAccessFile("data/metadata.csv","r");
				BufferedReader ram = new BufferedReader(new FileReader(
						"data/metadata.csv"));

				BufferedWriter bw = new BufferedWriter(new FileWriter("data/temp.csv",true));
			
			
				String line;
				StringTokenizer st;
			
				while ((line = ram.readLine()) != null) {

					st = new StringTokenizer(line, ",", false);

					if (st.nextToken().equals(table)) {
				
						if (st.nextToken().equals(col)) {

							String updatedLine = this.UpdateLine(line, 4, "true");
							bw.write(updatedLine + "\n");
						} else {
							bw.write(line + "\n");
						}

						}

						else {
	 
							bw.write(line + "\n");
						}
					}
			
				bw.flush();
				ram.close();
				bw.close();
				File oldFile = new File("data/metadata.csv");
			oldFile.delete();
			     
				File newFile = new File("data/temp.csv");
			      newFile.renameTo(oldFile);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**
		 * modifies a metadata entry to change some property
		 *
		 * @param line the entry to be updated
		 * @param position the index of the position to be modified
		 * @param value the new value
		 * @return returns a new line
		 */
		public String UpdateLine(String Line, int position, String value) {
			String[] split = Line.split(",");
			split[position] = value;

			String result = "";
			for (int i = 0; i < (split.length - 1); i++) {
				result += ((split[i] + ","));

			}
			result += (split[split.length - 1]);

			return result;

		}
		/**
		 * inserts all records of a specified column in a BTree
		 *
		 * @param tree the tree to insert in
		 * @param tableName the name of the table
		 * @param columnName the name of the column
		 */

		private void insertRecordsinTree(BTree tree, String tableName,
				String columnName) {

			int columnIndex = getColumnIndex(tableName, columnName);
			int pageCounter = 1;
			BufferedReader rdr;
			String parsedLine;
			StringTokenizer st;
			try {
				loop: while (true) {
					rdr = new BufferedReader(new FileReader("data/" + tableName
							+ pageCounter + ".csv"));
					for (int i = 0; i < MaximumRowsCountinPage; i++) {
						if ((parsedLine = rdr.readLine()) == null)
							break loop;
						else {
							st = new StringTokenizer(parsedLine, ",", false);
							for (int j = 0; j < columnIndex; j++)
								st.nextToken();
							{

								String temp = st.nextToken();
								ArrayList<String> check;
								if ((check = (ArrayList<String>) tree.find(temp)) != null) { // will
																								// check
																								// if
																								// the
																								// value
																								// is
																								// already
																								// in
																								// the
																								// tree
																								// to
																								// handle
																								// duplicates

									check.add(pageCounter + "," + i);
									tree.insert(temp, check, false);
								} else { // if the key is new we just insert a new
											// link with its value
									ArrayList<String> X = new ArrayList<String>();
									X.add(pageCounter + "," + i);
									tree.insert(temp, X, false);
								}

								// 3ATEF type has to be entered correctly
							}
						}
					}

				}
				rdr.close();

			} catch (IOException e) {

			}

		}
		/**
		 * gets the index of a column in a table
		 *
		 * @param tableName The name of the table
		 * @param colName the name of the column
		 * @return the index of the column
		 */

		private int getColumnIndex(String tableName, String colName) {
			try {
				BufferedReader rdr = new BufferedReader(new FileReader(
						"data/metadata.csv"));

				String line;
				StringTokenizer st;
				int counter = 0;
				while ((line = rdr.readLine()) != null) {
					st = new StringTokenizer(line, ",", false);
					if (st.nextToken().equals(tableName))

						if (st.nextToken().equals(colName))
							return counter;
						else
							counter++;

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return -1;
		}
		/**
		 * adds the metadata of a newly created file
		 *
		 * @param tName The name of the table
		 * @param types the types of all columns
		 * @param refs the references of each columns
		 * @param key the key column
		 */
		public void addMetaData(String tName, Hashtable<String, String> types,
				Hashtable<String, String> refs, String key) {

			try {

				BufferedWriter wrtr = new BufferedWriter(new FileWriter(
						"data/metadata.csv", true));

				Enumeration<String> typeList = types.keys();
				Enumeration<String> refList = refs.keys();
				String column;
				String type;
				String ky;
				String ref;
				String indexed;
				while (typeList.hasMoreElements()) {
					column = typeList.nextElement();
					type = types.get(column);
					indexed = ky = Boolean.toString(column == key);
					ref = (refs.get(column) == null ? "null" : refs.get(column));
					wrtr.write(tName + "," + column + "," + type + "," + ky + ","
							+ indexed + "," + ref + "\n");

				}
				wrtr.flush();
				wrtr.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**
		 * creates a new file
		 *
		 * @param name The name of the table
		 * @param n the page number
		 */

		public void CreateFile(String name, String n) {
			try {
				FileWriter w = new FileWriter("data/" + name + n + ".csv");
				w.flush();
				w.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/**
		 * checks wether a table exists or not
		 *
		 * @param tableName the name of the table
		 * @return wether the table exists or not
		 */

		public boolean tableExists(String tableName) {
			try {
				BufferedReader rdr = new BufferedReader(new FileReader(
						"data/metadata.csv"));
				StringTokenizer st;
				String line;
				while ((line = rdr.readLine()) != null) {
					st = new StringTokenizer(line, ",", false);
					if (st.nextToken().equals(tableName)) {
						rdr.close();
						return true;
					}
				}
				rdr.close();
				return false;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return false;

		}
		/**
		 * gets the primary key of a table
		 *
		 * @param tableName The name of the table
		 * @return the primary key of the table
		 */

		public String getPrimaryKey(String tableName) {
			try {
				BufferedReader rdr = new BufferedReader(new FileReader(
						"data/metadata.csv"));
				StringTokenizer st;
				String line;
				String X = "";
				while ((line = rdr.readLine()) != null) {
					st = new StringTokenizer(line, ",", false);
					if (st.nextToken().equals(tableName)) {

						if (line.split(",")[3].equals("true")) {
							rdr.close();
							return line.split(",")[1];
						}
					}
				}
				rdr.close();
				return "";

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return "";
		}
		/**
		 * checks whether an inserted value is in correct format
		 *
		 * @param type the format
		 * @param value the inserted value
		 * @param colName the name of the column
		 * @return whether the type is correct or not
		 */

		public boolean typeCheck(String type, String value, String colName) {
			if (type.equals("java.lang.Integer")) {
				try {
					Integer.parseInt(value);

				} catch (NumberFormatException e) {
					System.out.println("U must Enter an Integer in " + colName);
					return false;
				}
			}
			if (type.equals("java.util.Date,")) {
				try {
					String[] split = type.split("-");
					if (split.length != 3) {
						System.out.println("U must Enter a proper date format in "
								+ colName);
						return false;
					}
					for (int i = 3; i < split.length; i++) {
						int X = Integer.parseInt(split[i]);
					}
				} catch (NumberFormatException e) {
					System.out
							.println("the values in the date cannot contain letters");
					return false;
				}
			}
			if (type.equals("java.lang.String")) {
				try {
					Integer.parseInt(value);
					System.out
					.println("The inserted value cant be an integer USE STRING !");
					return false;

				} catch (NumberFormatException e) {

					return true;
				}
			}
			return true;
		}
		/**
		 * gets a specific BTree
		 *
		 * @param strTableName The name of the table
		 * @param col the name of the column
		 * @return the BTree
		 */

		public BTree getTree(String strTableName, String col) {
			BTree tree = null;

			try {
				long recid = recmanager.getNamedObject(strTableName + "," + col);
				if (recid != 0) {
					tree = BTree.load(recmanager, recid);
					recmanager.commit();
				}
			} catch (IOException e) {
				System.out.println("ERROR");
			}
			return tree;
		}
		/**
		 * inserts a record into a table
		 *
		 * @param strTableName The name of the table
		 * @param htblColNameValue the values of columns
		 * @throws DBAppException
		 */

		public void insertIntoTable(String strTableName,
				Hashtable<String, String> htblColNameValue) throws DBAppException {
		if(!tableExists(strTableName))
			throw new DBAppException("There is no table by the name "+strTableName);
		
			long recid;
			BTree tree;
			Enumeration<String> keys = htblColNameValue.keys();
			String col;

			// insert record and return it's place
			String place = insertRecordTable(strTableName, htblColNameValue);
			if (place == "keyExist")
				throw new DBAppException(
						"The primary key already exists in the table");
			if (place == "wrong values")
				throw new DBAppException("");

			if (place == "invalid")
				throw new DBAppException("invalid column names");
			// puts indexed columns in their Btrees //we can use insert record in
			// tree ?
			try {
				while (keys.hasMoreElements()) {
					col = keys.nextElement();
					recid = recmanager.getNamedObject(strTableName + "," + col);
					if (recid != 0)
					// or use insertRecordsinTree(BTREE,col,value) ;
					{
						tree = BTree.load(recmanager, recid);
						String value = htblColNameValue.get(col);
						ArrayList<String> X = (ArrayList<String>) tree.find(value);
						if (X == null) {
							ArrayList<String> temp = new ArrayList<String>();
							temp.add(place);
							tree.insert(htblColNameValue.get(col), temp, false);
						} else {
							X.add(place);
							tree.insert(htblColNameValue.get(col), X, false);
						}
						recmanager.commit();
					}
				}

			} catch (IOException e) {
				System.out.println("ERROR");
			}

		}
		/**
		 * Checks if the record is valid to insert in page and inserts it
		 *
		 * @param table The name of the table
		 * @param colval the values of columns
		 * @return returns the index
		 * @throws DBAppException
		 */

		public String insertRecordTable(String table,
				Hashtable<String, String> colval) throws DBAppException {
			try {
	 
				String primaryKey = getPrimaryKey(table);
				BTree tree = getTree(table, primaryKey);
				String check = colval.get(primaryKey);
				if(check!=null) {
				if (tree.find(check) != null) {
					return "keyExist";
				}
				}
				Set<String> keys = colval.keySet();
				int keysSize = keys.size();
				int columnnumbers = 0;
				BufferedReader rdr = new BufferedReader(new FileReader(
						"data/metadata.csv"));
				StringTokenizer st;
				String line;

				// check cols are correct
				while ((line = rdr.readLine()) != null) {
					st = new StringTokenizer(line, ",", false);
					if (st.nextToken().equals(table)) {
						columnnumbers++;
						String H = ((String)st.nextElement());
						if (!keys.contains(H)) {
						
								return "invalid";
							}
							else {
								String type = st.nextToken();
								String value = colval.get(H);
								if (!typeCheck(type, value, H))
									return "wrong values";
						
							}
						}
				
				}
	 if(columnnumbers != keys.size()){
			throw new DBAppException("U entered too many columns");
	 }
				// insert---------------------------------------------------------
				rdr.close();
				int pagecounter = 1;
				int rowcounter;
				try {
					loop: while (true) {
						rdr = new BufferedReader(new FileReader("data/" + table
								+ pagecounter + ".csv"));
						for (int i = 0; i < MaximumRowsCountinPage; i++) {
							if (rdr.readLine() == null) {
								rowcounter = i;
								break loop;
							}

						}
						pagecounter++;
					}

					WriteInTable(table, pagecounter, colval);
					return pagecounter + "," + rowcounter;
				} catch (FileNotFoundException e) {
					CreateFile(table, ((Integer) pagecounter).toString());
					WriteInTable(table, pagecounter, colval);
					rdr.close();
					e.printStackTrace();
					return pagecounter + "," + 0;
				}

			} catch (IOException e) {
				System.out.println("here");
				return "invalid";
			}

		}
		/**
		 * Writes records in a file
		 *
		 * @param table The name of the table
		 * @param pagecounter the page number
		 * @param colval the values of the columns
		 */

		public void WriteInTable(String table, int pagecounter,
				Hashtable<String, String> colval) {
			try {
				BufferedWriter wrtr = new BufferedWriter(new FileWriter("data/"
						+ table + pagecounter + ".csv", true));
				Enumeration<String> values = colval.elements();
				String row = "";

				while (values.hasMoreElements()) {
					row += values.nextElement();
					if (values.hasMoreElements())
						row += ",";
				}

				wrtr.write(row + "\n");
				wrtr.flush();
				wrtr.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		public static void saveAll(){
			return;
		}
		/**
		 * returns an iterator of some records specified 
		 *
		 * @param strTableName The name of the table
		 * @param htblColNameValue the values of the columns wanted
		 * @param strOperator the operator to be applied on the records wanted,either 'and' or 'or'
		 * @return an iterator of the records found
		 * @throws DBAppException
		 * @throws IOException
		 * @throws FileNotFoundException
		 */
		public Iterator<String> selectFromTable(String strTable,
				Hashtable<String, String> htblColNameValue, String strOperator)
				throws DBAppException, IOException, FileNotFoundException, DBAppException {
			if(!tableExists(strTable))
				throw new DBAppException("Table does not exist");
			ArrayList<String> result = new ArrayList<String>();
			int currentRecords = getCurrentRecords(strTable);
			int files = (int) Math.ceil((double) currentRecords
					/ (double) MaximumRowsCountinPage);
			if (htblColNameValue.size() == 1) {
				// only one condition so no need to strOperator
				Set<String> k = htblColNameValue.keySet();
				String l = k.toString();
				String colName = l.substring(1, l.length() - 1);
				String colVal = htblColNameValue.get(colName);
				if (ifIndexed(strTable, colName)) {
					// 5er we barka
					BTree tree = getTree(strTable, colName);
					result=  getRecordsInTree1(strTable, tree, colVal);
					if(result.size()==0)
						throw new DBAppException("No valid records");
					else  return result.iterator();
					// return result.iterator();
				} else {
					// iterate all records
					BufferedReader str = null;
					String s = "";

					for (int i = 1; i <= files; i++) {
						str = new BufferedReader(new FileReader("data/" + strTable
								+ i + ".csv"));
						while ((s = str.readLine()) != null) {
							int colIndex = this.getColumnIndex(strTable, colName);
							String[] split = s.split(",");
							if (split[colIndex].equals(colVal)) {
								result.add(s);
							}
						}
						str.close();
					}

					if(result.size()==0)
						throw new DBAppException("No valid records");
					else  return result.iterator();
				}
			} else {
				Set<String> k = htblColNameValue.keySet();
				String[] keys = new String[2];
				String colName1 = "";
				String colName2 = "";
				int count = 0;
				for (String k2 : k) {
					keys[count] = k2;
					count++;
				}
				colName1 = keys[0];
				colName2 = keys[1];
				String colVal1 = htblColNameValue.get(colName1);
				String colVal2 = htblColNameValue.get(colName2);
				int colIndex1 = this.getColumnIndex(strTable, colName1);
				int colIndex2 = this.getColumnIndex(strTable, colName2);

				if ((!ifIndexed(strTable, colName1) && !ifIndexed(strTable,
						colName2))
						|| (!ifIndexed(strTable, colName1) && strOperator
								.toLowerCase().equals("or"))
						|| (!ifIndexed(strTable, colName2) && strOperator
								.toLowerCase().equals("or"))) {
					// el two columns are not indexed yob2a iterate kol el files
					BufferedReader str = null;
					String s = "";

					for (int i = 0; i < files; i++) {
						int count2 = i + 1;
						str = new BufferedReader(new FileReader("data/" + strTable
								+ count2 + ".csv"));
						while ((s = str.readLine()) != null) {
							String[] split = s.split(",");
							if (strOperator.toLowerCase().equals("or")) {

								if (split[colIndex1].equals(colVal1)
										|| split[colIndex2].equals(colVal2)) {

									result.add(s);

								}
							} else if (split[colIndex1].equals(colVal1)
									&& split[colIndex2].equals(colVal2)) {
								result.add(s);

							}
						}
						str.close();
					}
					// str.close();
					if(result.size()==0)
						throw new DBAppException("No valid records");
					else  return result.iterator();
				}
				if (ifIndexed(strTable, colName1) && ifIndexed(strTable, colName2)) {

					BTree tree1 = getTree(strTable, colName1);
					BTree tree2 = getTree(strTable, colName2);
					ArrayList<String> rec1 = getRecordsInTree1(strTable, tree1,
							colVal1);
					ArrayList<String> rec2 = getRecordsInTree1(strTable, tree2,
							colVal2);
					if (strOperator.toLowerCase().equals("or")) {
						result.addAll(rec1);
						for (String ll : rec2) {
							if (!result.contains(ll))
								result.add(ll);
						}
					} else {
						for (String ll : rec2) {
							if (rec1.contains(ll))
								result.add(ll);
						}
					}
					if(result.size()==0)
						throw new DBAppException("No valid records");
					else  return result.iterator();
				} else if ((!ifIndexed(strTable, colName1)
						&& strOperator.toLowerCase().equals("and") && ifIndexed(
							strTable, colName2))
						|| ((!ifIndexed(strTable, colName2)
								&& strOperator.toLowerCase().equals("and") && ifIndexed(
									strTable, colName1)))) {
					if (ifIndexed(strTable, colName1)) {
						BTree tree1 = getTree(strTable, colName1);
						ArrayList<String> rec1 = getRecordsInTree1(strTable, tree1,
								colVal1);
						for (String ss : rec1) {
							String[] split = ss.split(",");
						
							if (split[colIndex2].equals(colVal2))
								result.add(ss);
						}
						 
			
						if(result.size()==0)
							throw new DBAppException("No valid records");
						else  return result.iterator();
					} else {
						BTree tree1 = getTree(strTable, colName2);
						ArrayList<String> rec2 = getRecordsInTree1(strTable, tree1,
								colVal2);
						for (String ss : rec2) {
							String[] split = ss.split(",");
							if (split[colIndex1].equals(colVal1))
								result.add(ss);
						}
						if(result.size()==0)
							throw new DBAppException("No valid records");
						else  return result.iterator();
					}
				} else {
					System.out.println("habl");
				}

			}
			return null;
		}

		// get the specific record by knowing its place
		/**
		 * gets a record knowing it's page number and row number
		 *
		 * @param tableName The name of the table
		 * @param place the page and row numbers
		 * @return the wanted record
		 * @throws IOException
		 */
		public String getRowByPlace(String tableName, String place)
				throws IOException {

			String[] split = place.split(",");
			String path = split[0];
			int row = Integer.parseInt(split[1]);

			BufferedReader str = new BufferedReader(new FileReader("data/"
					+ tableName + path + ".csv"));
			String s = "";
			for (int i = 0; i < row; i++)
				str.readLine();
			String s2 = str.readLine();
			str.close();
			return s2;
		}

		// get the specific record by knowing its place
		
		public String getRowByPlace1(String place) throws IOException {
			String[] split = place.split(",");
			String path = split[0];
			int row = Integer.parseInt(split[1]);
			BufferedReader str = new BufferedReader(new FileReader(path));
			for (int i = 0; i < row; i++) {
				str.readLine();
			}
			str.close();
			return str.readLine();
		}

	

		// get Current Records
		/**
		 * gets the number of records in a table
		 *
		 * @param strTableName The name of the table
		 * @return the number of records
		 */
		public int getCurrentRecords(String strTableName) throws IOException {
			BufferedReader str = new BufferedReader(new FileReader(
					"data/metadata.csv"));
			String s = "";
			while ((s = str.readLine()) != (null)) {
				String split[] = s.split(",");
				if (split[0].equals(strTableName) && split[3].equals("true")) {
					BTree tree = getTree(strTableName, split[1]);
					str.close();
					return tree.size();
				}
			}
			str.close();
			return -1;
		}
		/**
		 * checks whether a column is indexed or not
		 *
		 * @param strTableName The name of the table
		 * @param col the name of the column
		 * @return whether the column is indexed or not
		 * @throws IOException
		 */

		public boolean ifIndexed(String strTableName, String col)
				throws IOException {
			BufferedReader str = new BufferedReader(new FileReader(
					"data/metadata.csv"));
			String s = "";
			while ((s = str.readLine()) != (null)) {
				String split[] = s.split(",");
				if (split[0].equals(strTableName) && split[1].equals(col)
						&& split[4].equals("true")) {
					str.close();
					return true;
				}
			}
			str.close();
			return false;

		}
		
		/**
		 * gets an iterator of some record in a tree
		 *
		 * @param tableName The name of the table
		 * @param tree the name of the tree
		 * @param key the value of the column wanted
		 * @throws IOException
		 */
		public Iterator<String> getRecordsInTree(String tableName, BTree tree,
				String key) throws IOException {
			ArrayList<String> result = new ArrayList<String>();

			try {
				ArrayList<String> arr = (ArrayList<String>) tree.find(key);
				for (String s : arr) {
					String row = getRowByPlace(tableName, s);
					result.add(row);
				}
				return result.iterator();
			} catch (Exception e) {
				System.out.println("habl f habl");
				return null;
			}

		}

		public ArrayList<String> getRecordsInTree1(String tableName, BTree tree,
				String key) throws IOException {
			ArrayList<String> result = new ArrayList<String>();
			try {
				ArrayList<String> arr = (ArrayList<String>) tree.find(key);
				for (String s : arr) {
					String row = getRowByPlace(tableName, s);
					result.add(row);
				}
				return result;
			} catch (Exception e) {
				System.out.println("habl f habl");
				return result;
			}

		}

		
		
		

		public static void main(String[] args) throws IOException, DBAppException {
			/*
			 * Hashtable<String,String>types=new Hashtable<String,String>();
			 * Hashtable<String,String>refs=new Hashtable<String,String>();
			 * Hashtable<String,String>ahmed=new Hashtable<String,String>();
			 * Hashtable<String,String>ashraf=new Hashtable<String,String>();
			 * Hashtable<String,String>zaza=new Hashtable<String,String>();
			 * types.put("Name", "String"); types.put("Age", "Integer");
			 * refs.put("Name", "some reference"); ahmed.put("Name", "moha");
			 * ahmed.put("Age", "17"); ashraf.put("Name", "ayman");
			 * ashraf.put("Age", "23"); zaza.put("Name", "zaza"); zaza.put("Age",
			 * "33");
			 * 
			 * try { DBApp app=new DBApp(); app.init(); //BufferedWriter wr=new
			 * BufferedWriter(new FileWriter("data/h2.csv",true));
			 * //wr.write("omar,14"); // wr.flush();
			 * 
			 * //app.createTable("fff", types,refs,"Name");
			 * //app.insertIntoTable("fff",ahmed);
			 * //app.insertIntoTable("fff",ashraf);
			 * //app.insertIntoTable("fff",zaza);
			 * 
			 * long recid=app.recmanager.getNamedObject("fff"+","+"Name"); BTree
			 * tree=BTree.load(app.recmanager, recid);
			 * System.out.println(tree.find("moha"));
			 * System.out.println(tree.find("zaza"));
			 * System.out.println(tree.find("ayman")); System.out.println("=)"); }
			 * catch (IOException e) { System.out.println("=["); }
			 */

			// } catch (Exception e) {
			// System.out.println("=[");
			// }
		
			DBApp dbEngine = new DBApp();
			dbEngine.init();
			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("ID", "java.lang.Integer");
			htblColNameType.put("Name", "java.lang.String");
			htblColNameType.put("Location", "java.lang.String");

			Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
			htblColNameRefs.put("ID", "null");
			htblColNameRefs.put("Name", "null");
			htblColNameRefs.put("Location", "null");

		//dbEngine.createTable("DepartmentFINAL2", htblColNameType,
			//htblColNameRefs, "ID");
			
			String tableName = "EmployeeFINAL2";
			htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("ID", "java.lang.Integer");
			htblColNameType.put("Name", "java.lang.String");
			htblColNameType.put("Dept", "java.lang.String");
			htblColNameType.put("Start_Date", "java.util.Date");

			htblColNameRefs = new Hashtable<String, String>();
			htblColNameRefs.put("ID", "null");
			htblColNameRefs.put("Name", "null");
			htblColNameRefs.put("Dept", "null");
			htblColNameRefs.put("Start_Date", "null");

		//dbEngine.createTable(tableName, htblColNameType, htblColNameRefs,
//		"ID");  
			 
//			 dbEngine.addMetaData("EmployeeY",htblColNameType,htblColNameRefs, "ID");
	  dbEngine.createIndex("EmployeeFINAL2", "Start_Date");
			dbEngine.saveAll();


		}
	}

}
