/*
 * IrxVerifyTesting.java
 *
 * Created on April 25, 2006, 7:53 PM
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



public class Main {

    private int strBUS_UNIT;
    private String sNDC_ID;
    private String sUPC_ID;
    private float fAmt;
    private char cSTATUS;
    final  static String driverClass   = "oracle.jdbc.driver.OracleDriver";
    final  static String connectionTEST = "jdbc:oracle:thin:@uxn.longs.com:1521:FDMTST01";
    final  static String connectionPROD = "jdbc:oracle:thin:@ux14.longs.com:1521:FDMPRD01";
           static String connectionURL;
    final  static String userID        = "irxuser";
    final  static String userPassword  = "irxuser";
    Connection con_test                =   null;
    Connection con_prod                =   null;
    long lndc_nbr;
    long lupc_nbr ;
    double dPrice;
    String inLine                      = null;
    long l_InsertCounter               = 0;
    long l_DrugNMCounter               = 0;
    public static String str_Env;
    long wsDrugNameChange              = 0;


    /** Creates a new instance of IrxVerifyTesting */
public Main() {


     try {
            System.out.print("Loading JDBC via Driver -> " + driverClass + "\n");
            Class.forName(driverClass).newInstance();

            System.out.print("  Connection to  -> " + connectionTEST + "\n");
            System.out.print("  Connection to  -> " + connectionPROD + "\n");

            this.con_test = DriverManager.getConnection(connectionTEST, userID, userPassword);
            this.con_prod = DriverManager.getConnection(connectionPROD, userID, userPassword);

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


public String ConvertDateToOracleFormat(String in_date) {

 SimpleDateFormat sdfInput  =  new SimpleDateFormat( "yyyyMMdd" );
 SimpleDateFormat sdfOutput =  new SimpleDateFormat ( "dd-MMM-yyyy" );
 Date date = null;
 try {
       date = sdfInput.parse( in_date );

 }  catch (ParseException e) {
        System.out.print("Date Formetter Fail \n");
    }
  return ( sdfOutput.format(date ) );
 }


public void InsertPricingRecord(long p_db_SITE_ID, String p_db_FILL_DT, int p_qty, long p_db_NDC_NBR, long p_db_RX_PRSCRPTN_NBR, long p_db_RX_SEQ_NBR, double p_db_MTRC_DECML_CST, double t_db_MTRC_DECML_CST, String p_db_UPDT_DTTM, String t_db_UPDT_DTTM) {


 Statement stmt   =   null;
 double ws_prod_cost = 0;
 double ws_test_cost = 0;
 double ws_delta     = 0;
 int InsertPriceResults = 0;
 String  Bus_Unit;
 String  str_ext_date = null;

     try {
          stmt = con_test.createStatement ();

          ws_prod_cost = p_db_MTRC_DECML_CST * p_qty;
          ws_test_cost = t_db_MTRC_DECML_CST * p_qty;
          ws_delta = ws_prod_cost - ws_test_cost;

          InsertPriceResults = stmt.executeUpdate(
                      "INSERT INTO IRXUSER.IRX_RECON_TBL " +
                       "(SITE_ID,"                 +
                        "FILL_DT,"                 +
                         "QTY,"                    +
                         "NDC_NBR,"                +
                         "RX_NBR,"                 +
                         "RX_SEQ,"                 +
                         "PROD_UNIT_COST,"         +
                         "TEST_UNIT_COST,"         +
                         "PROD_COST,"              +
                         "TEST_COST,"              +
                         "DELTA,"                  +
                         "PROCESS_DT,"             +
                         "PROD_COST_UPDT_TS,"      +
                         "TEST_COST_UPDT_TS)"      +
                        " VALUES ("                +
                        p_db_SITE_ID               + ",'"   +
                        p_db_FILL_DT               + "',"   +
                        p_qty                      + ","    +
                        p_db_NDC_NBR               + ","    +
                        p_db_RX_PRSCRPTN_NBR       + ","    +
                        p_db_RX_SEQ_NBR            + ","    +
                        p_db_MTRC_DECML_CST        + ","    +
                        t_db_MTRC_DECML_CST        + ","    +
                        ws_prod_cost               + ","    +
                        ws_test_cost               + ","    +
                        ws_delta                   + ","    +
                        "sysdate"                  + ",'"    +
                        p_db_UPDT_DTTM             + "','"    +
                        t_db_UPDT_DTTM             + "')"  );


          l_InsertCounter++;

          stmt.close();

        } catch (SQLException e) {
             e.printStackTrace();


             System.out.print ( "ERROR ---> INSERT on Recon Table -> " + p_db_SITE_ID +  " NDC " + p_db_NDC_NBR + "Price " + p_db_RX_PRSCRPTN_NBR + "/n");

        }
}


public void FindFillRecordOnTest(long db_SITE_ID, String db_FILL_DT, int db_QTY, long db_NDC_NBR, long db_RX_PRSCRPTN_NBR, long db_RX_SEQ_NBR , double db_MTRC_DECML_CST, String db_UPDT_DTTM) {

 int rowNumber           = 0;
 double t_db_MTRC_DECML_CST           = 0;
 String t_db_UPDT_DTTM   = null;
 Statement stmt2         = null;
 ResultSet rset2         = null;
 double d_old_price      = 0.0;
 String d_old_eff_dt     = null;
 String str_ext_date     = null;

  String SQL_stmt2="select  MTRC_DECML_CST, to_char(b.UPDT_DTTM, 'dd-mon-yy') as UPDT_DTTM " +
  "from IRX_FILLS a, " +
       "irx_ndc_price b " +
   " where a.ndc_nbr = b.ndc_nbr " +
   " and b.eff_dt = (select max(c.eff_dt) from irx_ndc_price c where c.eff_dt <= a.fill_dt and c.ndc_nbr = b.ndc_nbr) " +
   " and to_char(a.fill_dt, 'dd-mon-yy') = " + "'10-oct-07'" +
   " and site_id = " + db_SITE_ID +
   " and a.ndc_nbr = " + db_NDC_NBR +
   " and rx_prscrptn_nbr =" + db_RX_PRSCRPTN_NBR +
   " and rx_seq_nbr = " + db_RX_SEQ_NBR;

 try {
      stmt2 = con_test.createStatement();
      rset2 = stmt2.executeQuery(SQL_stmt2);

      rset2.next();
      rowNumber = rset2.getRow();

      if ( rowNumber > 0 ) {
          t_db_MTRC_DECML_CST = rset2.getDouble("MTRC_DECML_CST");
          t_db_UPDT_DTTM      = rset2.getString("UPDT_DTTM");
           InsertPricingRecord(db_SITE_ID, db_FILL_DT, db_QTY, db_NDC_NBR, db_RX_PRSCRPTN_NBR, db_RX_SEQ_NBR, db_MTRC_DECML_CST, t_db_MTRC_DECML_CST, db_UPDT_DTTM, t_db_UPDT_DTTM);
  }
      else {
                  }


      stmt2.close();
      rset2.close();


      } catch (SQLException e) {
          e.printStackTrace();
         }


}


public void Main() {

int    i                      = 2;
String SQL_stmt1              = null;
long    db_RX_PRSCRPTN_NBR    = 0;
long    db_NDC_NBR            = 0;
long    db_SITE_ID            = 0;
int     db_RX_SEQ_NBR         = 0;
int     db_QTY                = 0;
double  db_MTRC_DECML_CST     = 0;
String  db_UPDT_DTTM          = null;
int rec_ct                    = 0;
String db_FILL_DT             = null;
Statement stmt1               = null;
ResultSet rset1               = null;
Statement stmt2               = null;
boolean b_first_time          = true;
long hld_str                  = 0;



  for ( i=2; i<999; i++ ) {

           SQL_stmt1="select SITE_ID, to_char(a.fill_dt,'dd-mon-yy') as fill_dt, a.MTRC_DCML_QTY, a.NDC_NBR, RX_PRSCRPTN_NBR,  RX_SEQ_NBR, MTRC_DECML_CST, to_char(b.UPDT_DTTM, 'dd-mon-yy') as UPDT_DTTM " +
        "from IRX_FILLS a, " +
            "irx_ndc_price b " +
        " where a.ndc_nbr = b.ndc_nbr " +
        " and b.eff_dt = (select max(c.eff_dt) from irx_ndc_price c where c.eff_dt <= a.fill_dt and c.ndc_nbr = b.ndc_nbr) " +
        " and to_char(a.fill_dt, 'dd-mon-yy') = '10-oct-07'"  +
        " and site_id=" + i;

      try {

            stmt1 = con_prod.createStatement();

            rset1 = stmt1.executeQuery(SQL_stmt1);
      //    if ( rset1.getRow() > 0 ) {
            while (rset1.next() ) {
                    db_SITE_ID         =  rset1.getLong("SITE_ID");
                    db_FILL_DT         =  rset1.getString("FILL_DT");
                    db_QTY             =  rset1.getInt("MTRC_DCML_QTY");
                    db_NDC_NBR         =  rset1.getLong("NDC_NBR");
                    db_RX_PRSCRPTN_NBR =  rset1.getLong("RX_PRSCRPTN_NBR");
                    db_RX_SEQ_NBR      =  rset1.getInt("RX_SEQ_NBR");
                    db_MTRC_DECML_CST  =  rset1.getDouble("MTRC_DECML_CST");
                    db_UPDT_DTTM       =  rset1.getString("UPDT_DTTM");
                    rec_ct++;
                    if (b_first_time) {
                      System.out.print("Processing Store " + db_SITE_ID + "\n");
                      b_first_time = false;
                      hld_str = db_SITE_ID;
                    }

                    if (hld_str != db_SITE_ID) {
                      System.out.print("Processing Store " + db_SITE_ID + "\n");
                      hld_str = db_SITE_ID;
                    }


                  FindFillRecordOnTest(db_SITE_ID, db_FILL_DT, db_QTY, db_NDC_NBR, db_RX_PRSCRPTN_NBR, db_RX_SEQ_NBR, db_MTRC_DECML_CST,db_UPDT_DTTM );
            }   //end-while


            stmt1.close();
            rset1.close();


         // }                             // end-if
      } catch (SQLException sqle) {
                sqle.printStackTrace();
                while (sqle != null) {
                      String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                      "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                      "SQLState: " + sqle.getSQLState() + "\n";
                      System.out.println(logMessage);
                      sqle = sqle.getNextException();
                }                       // end -while

        }                               // end-catch


  }                                     // end-for-loop
    System.out.print("Records Written " + rec_ct + "\n");


}

public static void main(String[] args)
                throws java.lang.InterruptedException {


     long elapsed_time;
     Date Start_time = new Date();
     System.out.print("IrxVerifyTesting Started " + Start_time.getTime() + "\n");
     Main PL = new Main();
     PL.Main();
     Date End_time = new Date();
     elapsed_time = End_time.getTime() - Start_time.getTime();

     System.out.print("IrxVerifyTesting Processing Time " + elapsed_time / 1000 + " seconds \n");
     System.out.print("IrxVerifyTesting Ended "  + End_time.getTime() + "\n");

  }
}

