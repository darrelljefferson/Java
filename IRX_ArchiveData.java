/*
 * IRX_Archivedata
 *
 * Created on July 25, 2006, 7:53 PM
 */

/**
 *
 * @author  Darrell Jefferson
 */
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.lang.Class;
import java.net.*;



public class IRX_ArchiveData {

    final  static String driverClass   = "oracle.jdbc.driver.OracleDriver";
    final  static String connectionTEST = "jdbc:oracle:thin:@uxn.longs.com:1521:FDMTST01";
           static String connectionURL;
    final  static String userID        = "irxuser";
    final  static String userPassword  = "irxuser";
    Connection con                     =   null;
    Connection con1                    =   null;
    Connection con2                    =   null;

    long l_DeleteCounter               = 0;
    public static String str_Env;

public IRX_ArchiveData() {

     try {
            System.out.print("Loading JDBC via Driver -> " + driverClass + "\n");
            Class.forName(driverClass).newInstance();

            System.out.print("  Connection to  -> " + connectionTEST + "\n");
            this.con = DriverManager.getConnection(connectionTEST, userID, userPassword);

            System.out.print(" Connected as  -> " + userID + "\n");

    } catch  (ClassNotFoundException e) {
                System.err.println("SQL Exception: " + e.getMessage());
                e.printStackTrace();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    e.printStackTrace();
    } catch (InstantiationException e) {
                e.printStackTrace();
    } catch (IllegalAccessException e) {
                e.printStackTrace();
    } catch (SQLException e) {
                e.printStackTrace();
    }
} /* End of Constructor */


 public void DeleteFillsRecords (String in_row_id) {
    Statement stmt          = null;
    int DeleteErrorResults  = 0;

   try {
      stmt = con.createStatement();

               DeleteErrorResults =  stmt.executeUpdate("DELETE FROM IRX_FILLS_ERRORS "  +
                                                        "WHERE ROWID = " + "'" + in_row_id + "'" );

   } catch (SQLException sqle) {
          sqle.printStackTrace();
           while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }
   }
 }

public void MainProcess() {

            Statement stmt1         = null;
            ResultSet rset1         = null;

            String  SQL_stmt1 = "select ROWID,"             +
                                        "FILL_DT, "         +
                                        "RX_PRSCRPTN_NBR,"  +
                                        "DRUG_NM        ,"  +
                                        "UPDT_DTTM      ,"  +
                                        "UPDT_USER_ID   ,"  +
                                        "NDC_NBR        ,"  +
                                        "CF_TOTE_NBR    ,"  +
                                        "MTRC_DCML_QTY  ,"  +
                                        "STATUS         ,"  +
                                        "ERROR_SEQ_NBR  ,"  +
                                        "SITE_ID        ,"  +
                                        "ERROR_CODE     ,"  +
                                        "PROCESS_NBR    ,"  +
                                        "RX_SEQ_NBR     ,"  +
                                        "CREATE_TS      ,"  +
                                        "CREATE_USER_ID "   +
                                  "from irx_fills_errors where create_ts < (sysdate - 545)";


 try {
      stmt1 = con.createStatement();
      rset1 = stmt1.executeQuery(SQL_stmt1);

      while (rset1.next()) {


             DeleteFillsRecords(rset1.getString("ROWID"));
             System.out.print(rset1.getString("Fill_dt") + " " + rset1.getLong("RX_PRSCRPTN_NBR") + " " + rset1.getLong("Site_id") + "\n");
             l_DeleteCounter ++;
      }
 } catch (SQLException sqle) {
          sqle.printStackTrace();
           while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

         }


   try {
           con.commit();
        }  catch (SQLException e) {
               e.printStackTrace();
        }


   System.out.print("Total Number of recs deleted is " +  l_DeleteCounter + "\n");
}




public static void main(String[] args)
                throws java.lang.InterruptedException {


     long elapsed_time;
     Date Start_time = new Date();

     System.out.print("IRX_Archive Delete Started " + Start_time.getTime() + "\n");
     IRX_ArchiveData AR = new IRX_ArchiveData();
     AR.MainProcess();
     Date End_time = new Date();
     elapsed_time = End_time.getTime() - Start_time.getTime();

     System.out.print("IRX_Archive Delete Processing Time " + elapsed_time / 1000 + " seconds \n");
     System.out.print("IRX_Archive Delete Ended "  + End_time.getTime() + "\n");

}
}
