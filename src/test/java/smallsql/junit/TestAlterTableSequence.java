/*
 * Created on 8.6.2021
 */
package smallsql.junit;
import org.junit.jupiter.api.*;

import smallsql.basicTestFrame;

import java.sql.*;

import static smallsql.junit.JunitTestExtended.*;


/**
 * @author Sean Nesbit
 * @author Riley Wartenberg
 * @author Sanjana Gundala
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAlterTableSequence extends BasicTestCase {

    private final String table = "junit_AlterTableSequence";
    private final int rowCount = 10;
    private Connection con; 

        public void setUp() {
        try {
            con = basicTestFrame.getConnection();
            Statement st = con.createStatement();
            st.execute("create table " + table + "(i int, v varchar(100))");
            st.execute("Insert into " + table + " Values(1,'abc')");
            st.execute("Insert into " + table + " Values(2,'bcd')");
            st.execute("Insert into " + table + " Values(3,'cde')");
            st.execute("Insert into " + table + " Values(4,'def')");
            st.execute("Insert into " + table + " Values(5,'efg')");
            st.execute("Insert into " + table + " Values(6,'fgh')");
            st.execute("Insert into " + table + " Values(7,'ghi')");
            st.execute("Insert into " + table + " Values(8,'hij')");
            st.execute("Insert into " + table + " Values(9,'ijk')");
            st.execute("Insert into " + table + " Values(10,'jkl')");
            st.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
    
    private void asserTableEqual(ResultSet rs, String seqType, int colNumber) throws Exception{
        int seq_fib1 = 1;
        int seq_fib2 = 1;
        int seq = 0;
        int counter = 0;
        while(rs.next()){  
            counter += 1;   
            //determine what the value should be   
            if(seqType.equalsIgnoreCase("FIB")){
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
            }
            else if(seqType.equalsIgnoreCase("INDEXA")){
                seq += 1;
            }
            else if(seqType.equalsIgnoreCase("INDEXD")){
                seq += 0;
            }
            else if(seqType.equalsIgnoreCase("EVENS")){
                seq += 2;
            }
            else if(seqType.equalsIgnoreCase("ODDS")){
                if(counter == 1)
                    seq -= 1;
                seq += 2;
            }
            //System.out.println("rs: " + rs.getObject(colNumber)+ " | seq: "+ seq);
            assertEquals(rs.getObject(colNumber), seq);
        }   

    }

    public int findColumnNumber(ResultSet rs, String colName) throws Exception{
        for (int i = rs.getMetaData().getColumnCount(); i >0; i--)
        {
            if (rs.getMetaData().getColumnName(i).equals(colName))
                return i;
        }
        return -1;
    }

    @Test
    public void testAddSequenceColumn() throws Exception {
        setUp();
        Statement st = con.createStatement();
        st.execute("Alter Table " + table + " add onecol int i FIB");
        ResultSet rs = st.executeQuery("Select * From "+ table + " order by onecol");
        asserTableEqual(rs, "FIB", rs.getMetaData().getColumnCount());
        st.close();
        rs.close();
    }

    @Test
    public void testAdd2SequenceColumn() throws Exception {
        Statement st = con.createStatement();
        st.execute("Alter Table " + table + " add twocolA int i FIB, twocolB int i INDEXA");
        ResultSet rs1 = st.executeQuery("Select * From "+ table + " order by twocolA");
        ResultSet rs2 = st.executeQuery("Select * From "+ table + " order by twocolB");
        asserTableEqual(rs1, "FIB", findColumnNumber(rs1, "twocolA"));
        asserTableEqual(rs2, "INDEXA", findColumnNumber(rs2, "twocolB"));
        st.close();
        rs1.close();
        rs2.close();
    }

}
