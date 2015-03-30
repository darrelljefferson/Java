/*
 * IRX_Fils_Load.java VERSION 1.0
 *
 * Created on June 26, 2006, 1:32 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

import java.util.logging.*;
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
import java.text.DateFormat;
import java.util.Date;


/**
 *
 * @author djefferson
 */

public class IRX_Fils_Load  {



 private static Logger logger = Logger.getLogger("IRX_FILLS");
;

final  static String driverClass   = "oracle.jdbc.driver.OracleDriver";
final  static String connectionTEST = "jdbc:oracle:thin:@uxn.longs.com:1521:FDMTST01";
final  static String connectionPROD = "jdbc:oracle:thin:@ux14.longs.com:1521:FDMPRD01";
static String        connectionURL;
final  static String userID        = "irxuser";
final  static String userPassword  = "irxuser";
final  static String InputFileName = "/longs/irx/ftp/inbound/current_billing.txt";
final  static String DELIM         = ";";

Connection con                      =   null;
Connection con1                     =   null;
Connection con2                     =   null;
Connection con3                     =   null;
Connection con4                     =   null;
Connection con5                     =   null;

long lndc_nbr;
long lInput_ct                     = 0;
long lError_ct                     = 0;
long lNonStore_ct                  = 0;
long LRx_Nbr;
long lErr_seq_nbr                  = 0;
long LSeq_nbr                      = 0;
public static String str_Env;



    /** Creates a new instance of IRX_Fils_Load */
 public IRX_Fils_Load() {
  String runBox;
  boolean b_YN = str_Env.equalsIgnoreCase("PROD") ;

     // runBox = GetHostName();

     if ( b_YN ) {
        connectionURL = connectionPROD;
     }  else {
               connectionURL = connectionTEST; }


         try {
            System.out.print("Loading JDBC via Driver -> " + driverClass + "\n");
            Class.forName(driverClass).newInstance();

            System.out.print("  Connection to  -> " + connectionURL + "\n");

            this.con = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con1 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con2 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con3 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con4 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con5 = DriverManager.getConnection(connectionURL, userID, userPassword);

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

}
public void InsertStoreOrder(String in_store_nbr, String in_fill_dt, String in_tote_nbr, String in_Ndc_nbr, String in_Rx_Nbr, String in_Drug_nm, String in_Qty, int in_seq_nbr) {

     Statement stmt3        = null;
     ResultSet rset1        = null;
     int InsertFillsResults = 0;
     long LStore_nbr;
     long LFill_dt;
     long LTote_nbr;
     long LNdc_nbr;
     String str_fill_dt = null;
     int  IQty;

     LStore_nbr = Long.parseLong(in_store_nbr);
     LTote_nbr  = Long.parseLong(in_tote_nbr);
     LNdc_nbr   = Long.parseLong(in_Ndc_nbr);
     LRx_Nbr    = Long.parseLong(in_Rx_Nbr);
     IQty       = Integer.parseInt(in_Qty);


     try {
            stmt3      = con.createStatement ();

            str_fill_dt = ConvertDateToOracleFormat( in_fill_dt);

            InsertFillsResults = stmt3.executeUpdate(
                      "INSERT INTO IRX_FILLS " +
                       "(PROCESS_NBR,"                +
                       "FILL_DT,"                     +
                       "SITE_ID,"                     +
                       "NDC_NBR,"                     +
                       "RX_PRSCRPTN_NBR,"             +
                       "RX_SEQ_NBR,"                  +
                       "CF_TOTE_NBR,"                 +
                       "MTRC_DCML_QTY,"               +
                       "PROCESS_FLG,"                 +
                       "CREATE_TS,"                   +
                       "CREATE_USER_ID,"              +
                       "UPDT_DTTM,"                   +
                       "UPDT_USER_ID)"                +
                        " VALUES ("                   +
                       "9999"        + ",'"           +
                       str_fill_dt   + "',"           +
                       LStore_nbr    + ","            +
                       LNdc_nbr      + ","            +
                       LRx_Nbr       + ","            +
                       in_seq_nbr    + ","            +
                       LTote_nbr     + ","            +
                       IQty          + ","            +
                       "'N'"         + ","            +
                       "SYSDATE"     + ","            +
                       "'IRXUSER'"   + ","            +
                       "SYSDATE"     + ","            +
                       "'IRXUSER'"   + ")" );
        stmt3.close();

        } catch (SQLException sqle) {
             logger.log(Level.INFO, "InsertStoreOrder() - Inserted failed in IRX_FILLS Table Site--> " + LStore_nbr + " Rx Nbr --> " + LRx_Nbr,sqle);
             // sqle.printStackTrace();
             while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

             InsertFillsErrorTable(LStore_nbr, str_fill_dt,  LTote_nbr, LNdc_nbr, LRx_Nbr, in_Drug_nm, IQty, in_seq_nbr);
        }

}

/*  --------------------------------------------------------------------------------
 *       Insert Fills with missing parent NDC price record
 *  --------------------------------------------------------------------------------
 */

public void InsertFillsErrorTable(long in_store_nbr, String in_fill_dt, long in_tote_nbr, long in_Ndc_nbr,  long in_Rx_Nbr, String in_Drug_nm, long in_Qty, int in_seq_nbr)  {


  Statement stmt4          = null;
  ResultSet rset4          = null;
  int  InsertFillsResults  = 0;
  String str_fill_dt       = null;

try {
           stmt4      = con1.createStatement ();
           lErr_seq_nbr++;

           // str_fill_dt = ConvertDateToOracleFormat( in_fill_dt);
           InsertFillsResults = stmt4.executeUpdate("INSERT INTO IRX_FILLS_ERRORS " +
                       "(PROCESS_NBR,"         +
                       "FILL_DT,"              +
                       "SITE_ID,"              +
                       "NDC_NBR,"              +
                       "RX_PRSCRPTN_NBR,"      +
                       "RX_SEQ_NBR,"           +
                       "ERROR_CODE,"           +
                       "ERROR_SEQ_NBR,"        +
                       "CF_TOTE_NBR,"          +
                       "DRUG_NM,"              +
                       "MTRC_DCML_QTY,"        +
                       "STATUS,"               +
                       "CREATE_TS,"            +
                       "CREATE_USER_ID,"       +
                       "UPDT_DTTM,"            +
                       "UPDT_USER_ID)"         +
                       " VALUES ("             +
                       "9999"           + ",'"    +
                       in_fill_dt       + "',"    +
                       in_store_nbr     + ","     +
                       in_Ndc_nbr       + ","     +
                       in_Rx_Nbr        + ","     +
                       in_seq_nbr       + ","     +
                       "306"            + ","     +
                       lErr_seq_nbr     + ","     +
                       in_tote_nbr      + ",'"    +
                       in_Drug_nm       + "',"    +
                       in_Qty           + ","     +
                       "'E'"            + ","     +
                       "SYSDATE"        + ","     +
                       "'IRXUSER'"        + ","     +
                       "SYSDATE"        + ","     +
                       "'IRXUSER'"      + ")" );

        logger.fine( "InsertFillsErrorTable() - Inserted Fill Error Table due to missing Parent NDC --> " + in_Ndc_nbr + " Site -> " + in_store_nbr + " Rx_Nbr-> " + in_Rx_Nbr);
        stmt4.close();

        } catch (SQLException sqle) {

              logger.log(Level.WARNING, "InsertFillsErrorTable() -  Failed insert for IRX_FILLS_ERROR TABLE Site-> " + in_store_nbr , sqle);

              // sqle.printStackTrace();
              while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

              try {
                stmt4.close();
              } catch (SQLException e) {
              }

        }


}

public void InsertNonStoreOrders(String in_Store_nbr, String in_Fill_dt, String  in_Tote_nbr, String in_NDC_nbr, String in_Rx_nbr, String in_Drug_nm, String in_qty, int iSeqNbr) {

Statement stmt4          = null;
ResultSet rset4          = null;
int  InsertFillsResults  = 0;

int   LSeq_nbr           = 0;
long  LStore_nbr;
long  LFill_dt;
long  LTote_nbr;
long  LNdc_nbr;
long  LRx_Nbr;
int   IQty;
String str_fill_dt = null;

     LStore_nbr = Long.parseLong(in_Store_nbr);
     LFill_dt   = Long.parseLong(in_Fill_dt);
     LNdc_nbr   = Long.parseLong(in_NDC_nbr);
     LRx_Nbr    = Long.parseLong(in_Rx_nbr);
     IQty       = Integer.parseInt(in_qty);


     try {
           stmt4      = con2.createStatement ();
           lErr_seq_nbr++;
           lNonStore_ct++;

           str_fill_dt = ConvertDateToOracleFormat( in_Fill_dt);
           InsertFillsResults = stmt4.executeUpdate("INSERT INTO IRX_FILLS_ERRORS " +
                       "(PROCESS_NBR,"         +
                       "FILL_DT,"              +
                       "SITE_ID,"              +
                       "NDC_NBR,"              +
                       "RX_PRSCRPTN_NBR,"      +
                       "RX_SEQ_NBR,"           +
                       "ERROR_CODE,"           +
                       "ERROR_SEQ_NBR,"        +
                       "CF_TOTE_NBR,"          +
                       "DRUG_NM,"              +
                       "MTRC_DCML_QTY,"        +
                       "STATUS,"               +
                       "CREATE_TS,"            +
                       "CREATE_USER_ID,"       +
                       "UPDT_DTTM,"            +
                       "UPDT_USER_ID)"         +
                       " VALUES ("             +
                       "9999"        + ",'"    +
                       str_fill_dt   + "',"    +
                       LStore_nbr    + ","     +
                       LNdc_nbr      + ","     +
                       LRx_Nbr       + ","     +
                       LSeq_nbr      + ","     +
                       "307"         + ","     +
                       lErr_seq_nbr  + ","     +
                       in_Tote_nbr   + ",'"    +
                       in_Drug_nm    + "',"    +
                       IQty          + ","     +
                       "'E'"         + ","     +
                       "SYSDATE"     + ","     +
                       "'IRXUSER'"   + ","     +
                       "SYSDATE"     + ","     +
                       "'IRXUSER'"   + ")" );

        System.out.print("INSERT INTO IRX_FILLS_ERRORS " +
                       "(PROCESS_NBR,"         +
                       "FILL_DT,"              +
                       "SITE_ID,"              +
                       "NDC_NBR,"              +
                       "RX_PRSCRPTN_NBR,"      +
                       "RX_SEQ_NBR,"           +
                       "ERROR_CODE,"           +
                       "ERROR_SEQ_NBR,"        +
                       "CF_TOTE_NBR,"          +
                       "DRUG_NM,"              +
                       "MTRC_DCML_QTY,"        +
                       "STATUS,"               +
                       "UPDT_DTTM,"            +
                       "UPDT_USER_ID)"         +
                       " VALUES ("             +
                       "9999"        + ",'"    +
                       str_fill_dt   + "',"    +
                       LStore_nbr    + ","     +
                       LNdc_nbr      + ","     +
                       LRx_Nbr       + ","     +
                       LSeq_nbr      + ","     +
                       "307"         + ","     +
                       lErr_seq_nbr  + ","     +
                       in_Tote_nbr   + ",'"    +
                       in_Drug_nm    + "',"    +
                       IQty          + ","     +
                       "'E'"         + ","    +
                       "SYSDATE"     + ","    +
                       "'IRXUSER'"   + ")" + "\n" );




        stmt4.close();

        } catch (SQLException sqle) {
             logger.log(Level.WARNING, "InsertNonStoreOrders() - Failed insert for IRX_FILLS_ERROR TABLE-> ", sqle);

            //  sqle.printStackTrace();
             while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

              try {
                stmt4.close();
              } catch (SQLException e) {
              }

        }
}

 public boolean ValidateStoreNbr(String in_Store_nbr) {

 Statement stmt2    =   null;
 ResultSet rset2    =   null;
 boolean bYN_Flag   =   true;
 String SQL_stmt2 = "Select loc_nbr from Stores where loc_nbr = " + in_Store_nbr;

   try {
       stmt2 = con3.createStatement();
       rset2 = stmt2.executeQuery(SQL_stmt2);

       rset2.next();

         if (rset2.getRow() > 0) {
            bYN_Flag = true;
         }
         else {
           bYN_Flag = false; }


       stmt2.close();
       rset2.close();

   } catch (SQLException sqle) {
             logger.log(Level.WARNING, "ValidateStoreNbr() - Select failed for Stores TABLE-> ", sqle);
             // sqle.printStackTrace();
             while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }


          try {
                stmt2.close();
                rset2.close();
              } catch (SQLException e) {
              }
   }



 return bYN_Flag;
 }
 public int  CheckforDuplicateNDC (String in_store, String in_fill_dt, String in_ndc, String in_Rx_nbr) {

 Statement stmt           =   null;
 ResultSet rset1          =   null;
 int       rowNumber      =   0;
 long      lNDC;
 long      lStore;
 int       iStore_ct     = 0;
 String str_fill_dt      = null;


     str_fill_dt = ConvertDateToOracleFormat( in_fill_dt);
     String SQL_stmt1 = "select count(*) from irx_fills where ndc_nbr =" + in_ndc +  " and RX_PRSCRPTN_NBR = " + in_Rx_nbr + " and fill_dt ='" + str_fill_dt + "'";

     try {
            stmt = con4.createStatement ();
            rset1 = stmt.executeQuery(SQL_stmt1);

      while (rset1.next())  {
            iStore_ct = rset1.getInt(1);
            // Logger.fine( in_store  + in_ndc + in_Rx_nbr + iStore_ct  + str_fill_dt);
      }
      rset1.close();
      stmt.close();


        } catch (SQLException sqle) {
             logger.log(Level.WARNING, "CheckforDuplicateNDC() - Select Failed Duplicate checking ", sqle);
            // sqle.printStackTrace();
             while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

           try {
                  rset1.close();
                  stmt.close();
           }  catch (SQLException e) {
              }

        }
  return iStore_ct + 1;
 }

public int  CheckForNonStoreDuplicateNDC (String in_store, String in_fill_dt, String in_ndc, String in_Rx_nbr) {

 Statement stmt     =   null;
 ResultSet rset1    =   null;
 int rowNumber      =   0;
 long lNDC;
 long lStore;
 int  iStore_ct     = 0;
 String str_fill_dt = null;

       str_fill_dt = ConvertDateToOracleFormat( in_fill_dt);
       String SQL_stmt1 = "select count(*) from irx_fills_errors where ndc_nbr =" + in_ndc + " and RX_PRSCRPTN_NBR = " + in_Rx_nbr + "and FILL_dt='" + str_fill_dt + "'" + " and site_id=" + in_store;

     try {
            stmt = con5.createStatement ();
            rset1 = stmt.executeQuery(SQL_stmt1);


      while (rset1.next())  {
            iStore_ct = rset1.getInt(1);

      }
      rset1.close();
      stmt.close();


        } catch (SQLException sqle) {
             // sqle.printStackTrace();
             while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }
             logger.log(Level.WARNING, "CheckForNonStoreDuplicateNDC() - Select Failed Duplicate checking ", sqle);
        }
  return iStore_ct + 1;
 }

 public String RemoveSpaces (String in_value) {

 int     len       = in_value.length();
 int     blank_pos = in_value.indexOf(" ", 1);
 String  num       = in_value.substring(0, (len - ((len - blank_pos) )));

 return num;

 }

 public String ConvertDateToOracleFormat(String in_date) {

 SimpleDateFormat sdfInput  =  new SimpleDateFormat( "yyyyMMdd" );
 SimpleDateFormat sdfOutput =  new SimpleDateFormat ( "dd-MMM-yyyy" );
 Date date = null;
 try {
       date = sdfInput.parse( in_date );

 }  catch (ParseException e) {
        logger.log(Level.WARNING, "ConvertDateToOracleFormat() - Date conversion Error ", e);
    }
  return ( sdfOutput.format(date ) );
 }


 public void MainProcess() {

// Logger.fine("MainProcess has started");
 StringTokenizer st = null;
 String  str_Store_nbr;
 String  str_Fill_dt;
 String  str_Tote_nbr;
 String  str_NDC_nbr;
 String  str_Prescr_nbr;
 String  str_Drug_nm;
 String  str_qty;
 int     iSeqNbr = 0;
 boolean bGoodStore;

  try {
       System.out.print("Create FileReader Obj " + InputFileName + " \n");
       FileReader InputFileReader = new FileReader(InputFileName);
       System.out.print("Create BufferReader Obj \n");
       BufferedReader inputStream = new BufferedReader(InputFileReader);

       String inLine = null;

       while ((inLine = inputStream.readLine()) != null) {
                 st = new StringTokenizer(inLine,DELIM);

              str_Store_nbr  = st.nextToken();
              str_Fill_dt    = st.nextToken();
              str_Tote_nbr   = st.nextToken();
              str_NDC_nbr    = st.nextToken();
              str_Prescr_nbr = st.nextToken();
              str_Drug_nm    = st.nextToken();
              str_qty        = st.nextToken();
              str_Store_nbr  = RemoveSpaces(str_Store_nbr);
              str_Tote_nbr   = RemoveSpaces(str_Tote_nbr);

              bGoodStore = ValidateStoreNbr(str_Store_nbr);
              lInput_ct++;

              if (bGoodStore) {
                  iSeqNbr =  CheckforDuplicateNDC(str_Store_nbr, str_Fill_dt, str_NDC_nbr, str_Prescr_nbr);

                  InsertStoreOrder(str_Store_nbr, str_Fill_dt,  str_Tote_nbr, str_NDC_nbr, str_Prescr_nbr, str_Drug_nm, str_qty, iSeqNbr);
              }
                 else {

                        iSeqNbr = CheckForNonStoreDuplicateNDC(str_Store_nbr, str_Fill_dt, str_NDC_nbr, str_Prescr_nbr);
                        InsertNonStoreOrders(str_Store_nbr, str_Fill_dt,  str_Tote_nbr, str_NDC_nbr, str_Prescr_nbr, str_Drug_nm, str_qty, iSeqNbr);
                 } // End If  Else
       } // End-While

  } catch (IOException e) {
             e.printStackTrace();
            }

            try {
              con.commit();
            }  catch (SQLException e) {
               e.printStackTrace();
               }

        }
public static void main(String[] args)
                throws java.lang.InterruptedException {


 try {
      FileHandler fh = new FileHandler("mylog.txt") ;
      logger.addHandler(fh);
     }
 catch (IOException e) {
     }


;
 logger.setLevel(Level.ALL);

 int exit_code  = 0;
 boolean b_cmp_test = false;
 String str_Test_mode = "-t";
 String str_Prod_mode = "-p";



 if ( args.length == 0 | args.length > 1 ) {
      System.out.print("Program does not have correct parms -t or -p \n");
      exit_code = -1;
      System.exit(exit_code);
 }
 else {

      b_cmp_test = args[0].equalsIgnoreCase(str_Test_mode);
      if ( b_cmp_test ) {
        System.out.print("Running in TEST mode \n");
        str_Env = "TEST";
      } else {
         b_cmp_test = args[0].equalsIgnoreCase(str_Prod_mode);
         if ( b_cmp_test ) {
            System.out.print("Running in PROD mode \n");
            str_Env = "PROD";
         } else {
                 System.out.print("Parameter is not -t or -p \n");
                 exit_code = -1;
                 System.exit(exit_code);
                 }   /* end - else */
      }
   }

long elapsed_time;
Date Start_time = new Date();
System.out.print("IRX_Fils_Load Logic Started " + Start_time.getTime() + "\n");

IRX_Fils_Load IRX = new IRX_Fils_Load();
IRX.MainProcess();
Date End_time = new Date();
elapsed_time = End_time.getTime() - Start_time.getTime();
System.out.print("IRX_Fils_Load Processing Time " + elapsed_time / 1000 + " seconds \n");
System.out.print("IRX_Fils_Load Ended "  + End_time.getTime() + "\n");

 }
}
