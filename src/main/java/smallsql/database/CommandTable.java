/* =============================================================
 * SmallSQL : a free Java DBMS library for the Java(tm) platform
 * =============================================================
 *
 * (C) Copyright 2004-2007, by Volker Berlin.
 *
 * Project Info:  http://www.smallsql.de/
 *
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by 
 * the Free Software Foundation; either version 2.1 of the License, or 
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but 
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public 
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, 
 * USA.  
 *
 * [Java is a trademark or registered trademark of Sun Microsystems, Inc. 
 * in the United States and other countries.]
 *
 * ---------------
 * CommandTable.java
 * ---------------
 * Author: Volker Berlin
 * 
 */
package smallsql.database;
import java.sql.*;

import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import smallsql.tools.language.Language;


final class CommandTable extends Command{

	final private Columns columns = new Columns();
	final private IndexDescriptions indexes = new IndexDescriptions();
    final private ForeignKeys foreignKeys = new ForeignKeys();
    final private int tableCommandType;
    private  List<String> orderByColumn = new ArrayList<String>();
    private  List<Integer> sequenceValue = new ArrayList<Integer>();
	
    CommandTable( Logger log, String catalog, String name, int tableCommandType ){
    	super(log);
        this.type = SQLTokenizer.TABLE;
        this.catalog = catalog;
        this.name = name;
        this.tableCommandType = tableCommandType;
    }
    

    /**
     * Add a column definition. This is used from the SQLParser.
     * 
     * @throws SQLException
     *             if the column already exist in the list.
     * @see SQLParser#createTable
     */
    void addColumn(Column column) throws SQLException{
        addColumn(columns, column);
    }
	
	
	void addIndex( IndexDescription indexDescription ) throws SQLException{
		indexes.add(indexDescription);
	}
	
	
    void addForeingnKey(ForeignKey key){
        foreignKeys.add(key);
    }

    void addOrderBy(String colname){
        this.orderByColumn.add(colname);
    }

    void addSequence(int tokenvalue){
        this.sequenceValue.add(tokenvalue);
    }
    
    
    void executeImpl(SSConnection con, SSStatement st) throws Exception{
        Database database = catalog == null ? 
                con.getDatabase(false) : 
                Database.getDatabase( catalog, con, false );
        switch(tableCommandType){
        case SQLTokenizer.CREATE:
            database.createTable( con, name, columns, indexes, foreignKeys );
            break;
        case SQLTokenizer.ADD:
            con = new SSConnection(con);
            //TODO disable the transaction to reduce memory use.
            Table oldTable = (Table)database.getTableView( con, name);
            
            // Request a TableLock and hold it for the completely ALTER TABLE command
            TableStorePage tableLock = oldTable.requestLock( con, SQLTokenizer.ALTER, -1);
            String newName = "#" + System.currentTimeMillis() + this.hashCode();
            try{
                Columns oldColumns = oldTable.columns;
                Columns newColumns = oldColumns.copy();
                for(int i = 0; i < columns.size(); i++){
                    addColumn(newColumns, columns.get(i));
                }
                
                Table newTable = database.createTable( con, newName, newColumns, oldTable.indexes, indexes, foreignKeys );
                StringBuffer buffer = new StringBuffer(256);
                buffer.append("INSERT INTO ").append( newName ).append( '(' );
                for(int c=0; c<oldColumns.size(); c++){
                    if(c != 0){
                        buffer.append( ',' );
                    }
                    buffer.append( oldColumns.get(c).getName() );
                }
                buffer.append( ")  SELECT * FROM " ).append( name );
                con.createStatement().execute( buffer.toString() );
                
                database.replaceTable( oldTable, newTable );
                
                //there are no sequences to update table with
                if(orderByColumn.size() == 0)
                    return;

                //adding a sequence
                for(int i = 0; i < columns.size(); i++){
                    Statement st1 = con.createStatement();
                    st1.execute("select * from " + name + " order by " + this.orderByColumn.get(i));
                    ResultSet rs = st1.getResultSet();

                    int seq_fib1 = 1;
                    int seq_fib2 = 1;
                    int seq = 0;
                    int counter = 0;

                    //for each row
                    while (rs.next()) {
                        StringBuffer updateBuffer = new StringBuffer(256);
                        updateBuffer.append("UPDATE ").append(name).append(" SET ").append(columns.get(i).getName());
                        counter += 1;
                        switch(sequenceValue.get(i)){
                            case 270: //Fib
                                if(counter <= 2){   
                                    seq = 1;
                                    seq_fib1 = 1;
                                    seq_fib2 = 1;
                                }
                                else{
                                    seq = seq_fib1 + seq_fib2;
                                    seq_fib1 = seq_fib2;
                                    seq_fib2 = seq;
                                }
                                break;
                            case 271: //Ascending
                                seq += 1;
                                break;
                            case 272: //Descending
                                seq = 0;
                                break;
                            case 273: //Evens
                                seq += 2;
                                break;
                            case 274: //Odds
                                if(counter == 1)
                                    seq -= 1;
                                seq += 2;
                                break;
                            default:
                                return;
                        }
                        updateBuffer.append(" = "+seq);
                        updateBuffer.append(" WHERE ");
                        for (int j = 0; j < oldColumns.size(); j++) {
                            if (rs.getObject(j+1) == null)
                                continue;
                            if(j != 0)
                                updateBuffer.append(" AND ");
                            updateBuffer.append(oldColumns.get(j).getName() + " = "+ rs.getObject(j+1));
                            
                        }
                        System.out.println(updateBuffer.toString());
                        con.createStatement().execute(updateBuffer.toString());
                    }
                }
                



            }catch(Exception ex){
                //Remove all from the new table
                try {
                    database.dropTable(con, newName);
                } catch (Exception ex1) {/* ignore it */}
                try{
                    indexes.drop(database);
                } catch (Exception ex1) {/* ignore it */}
                throw ex;
            }finally{
                tableLock.freeLock();
            }
            break;
        default:
            throw new Error();
        }
    }


    private void addColumn(Columns cols, Column column) throws SQLException{
        if(cols.get(column.getName()) != null){
            throw SmallSQLException.create(Language.COL_DUPLICATE, column.getName());
        }
        cols.add(column);
    }
}
