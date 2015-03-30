/*
 * IrxPricingLoad.java
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



public class IrxPricingLoad {

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
    final  static String InputFileName = "/longs/irx/ftp/inbound/mailrx_prices.txt1";
    final  static String DELIM         = ";";
    Connection con                     =   null;
    Connection con2                    =   null;
    Connection con3                    =   null;
    Connection con4                    =   null;
    long lndc_nbr;
    long lupc_nbr ;
    double dPrice;
    String inLine                      = null;
    long l_InsertCounter               = 0;
    long l_DrugNMCounter               = 0;
    public static String str_Env;
    long wsDrugNameChange              = 0;


    /** Creates a new instance of IrxPricingLoad */
public IrxPricingLoad() {
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
            this.con3 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con2 = DriverManager.getConnection(connectionURL, userID, userPassword);
            this.con4 = DriverManager.getConnection(connectionURL, userID, userPassword);
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


public void InsertPricingRecord(String In_Date,String In_Bus_Unit, String In_ndc_nbr, String In_upc_nbr, String In_Price, String In_status) {


 Statement stmt   =   null;
 int InsertPriceResults = 0;
 String  Bus_Unit;
 String  str_ext_date = null;


     try {
          stmt = con.createStatement ();
          lndc_nbr = Long.parseLong(In_ndc_nbr);
          lupc_nbr = Long.parseLong(In_upc_nbr);
          dPrice = Double.parseDouble(In_Price);
          str_ext_date = ConvertDateToOracleFormat (In_Date);

          InsertPriceResults = stmt.executeUpdate(
                      "INSERT INTO IRX_NDC_PRICE " +
                       "(BUS_UNIT,"            +
                       "NDC_NBR,"              +
                       "UPC_NBR,"              +
                       "EFF_DT,"               +
                       "STATUS,"               +
                       "MTRC_DECML_CST,"       +
                       "NOTES,"                +
                       "UPDT_DTTM,"            +
                       "UPDT_USER_ID,"         +
                       "CREATE_TS,"            +
                       "CREATE_USER_ID)"       +
                        "VALUES ("             +
                        "'CENTRAL'"    + ","   +
                        lndc_nbr       + ","   +
                        lupc_nbr       + ",'"  +
                        str_ext_date   + "',"  +
                         "'A'"         + ","   +
                        dPrice         + ","   +
                        "NULL"         + ","   +
                        "SYSDATE"      + ","   +
                        "'IRXUSER'"    + ","   +
                        "SYSDATE"      + ","   +
                        "'IRXUSER')" );

          l_InsertCounter++;

          stmt.close();

        } catch (SQLException e) {
             e.printStackTrace();

             System.out.print ( "ERROR ---> INSERT FAILED FOR NDC PRICE -> " + lndc_nbr +  " Effdt " + str_ext_date + "Price "
                  + dPrice + "\n" );
        }
}

public void InsertDrugNmTable (String in_ndc_nbr, String in_label_name) {

 Statement stmt   =   null;
 int InsertPriceResults = 0;
 String  Bus_Unit;

 try {
          stmt = con3.createStatement ();

          InsertPriceResults = stmt.executeUpdate(
                      "INSERT INTO IRX.IRX_DRUG_NM " +
                       "(NDC_NBR,"             +
                       "DRUG_NM,"              +
                       "CREATE_TS,"            +
                       "CREATE_USER_ID,"       +
                       "UPDT_TS,"              +
                       "UPDT_USER_ID)"         +
                       "VALUES ("              +
                        in_ndc_nbr             + ",'"   +
                        in_label_name         + "',"    +
                        "SYSDATE"      + ","   +
                        "'TIRXUSER'"    + ","   +
                        "SYSDATE"      + ","   +
                        "'TIRXUSER')" );

          l_DrugNMCounter++;

          System.out.print("Insert new drug " + in_ndc_nbr + " Drug Name " + in_label_name + "\n" );
          stmt.close();

        } catch (SQLException e) {
             e.printStackTrace();

             System.out.print ( "ERROR ---> INSERT FAILED FOR DRUG NAME  -> " + in_ndc_nbr +  " Drug Name " + in_label_name + "\n" );
        }


}


public boolean DetermineIfPriceExist (String in_ndc_nbr, String in_price, String in_date, String label_name) {

 int rowNumber           = 0;
 double dPrice           = 0;
 Statement stmt2         = null;
 ResultSet rset2         = null;
 double d_old_price      = 0.0;
 String d_old_eff_dt     = null;
 String str_ext_date     = null;

 dPrice = Double.parseDouble(in_price) ;
 str_ext_date = ConvertDateToOracleFormat (in_date);



 String SQL_stmt2 = "select mtrc_decml_cst, eff_dt from irx_ndc_price a where " +
      " a.ndc_nbr =" + in_ndc_nbr + " and a.eff_dt = (select max(b.eff_dt) from irx_ndc_price b where a.ndc_nbr = b.ndc_nbr)";

 try {
      stmt2 = con.createStatement();
      rset2 = stmt2.executeQuery(SQL_stmt2);

      rset2.next();
      rowNumber = rset2.getRow();

      if ( rowNumber > 0 ) {
           DetermineIfDrugNameChange(in_ndc_nbr, label_name);
           d_old_price = rset2.getDouble(1);
           d_old_eff_dt = rset2.getString(1);  }
      else {
            if ( !DetermineIfDrugNamePresent(in_ndc_nbr) )
                 InsertDrugNmTable(in_ndc_nbr , label_name ) ;
      }


      stmt2.close();
      rset2.close();


      } catch (SQLException e) {
          e.printStackTrace();
         }


 if ( rowNumber > 0 && d_old_price == dPrice )
      return (true);
  else
     return (false);

}

/*--------------------------------------------------------------------------------*
 *   Determine if parnet table contains new ndc number                            *
 *                                                                                *
 *--------------------------------------------------------------------------------*/

 public void DetermineIfDrugNameChange(String in_ndc_nbr, String label_name)   {

 int rowNumber           = 0;
 Statement stmt2         = null;
 Statement stmt3         = null;
 ResultSet rset2         = null;


  String SQL_stmt2 = "select drug_nm from irx.irx_drug_nm where  ndc_nbr =" + in_ndc_nbr;

try {
      stmt2 = con.createStatement();
      stmt3 = con4.createStatement();
      rset2 = stmt2.executeQuery(SQL_stmt2);

      rset2.next();
      rowNumber = rset2.getRow();
      if ( rowNumber > 0 ) {

          String old_Drug_name = rset2.getString(1);

          if ( !old_Drug_name.equalsIgnoreCase(label_name) ) {
              int UpdatePriceResults = stmt3.executeUpdate("UPDATE IRX.IRX_DRUG_NM "               +
                                                          "SET DRUG_NM= "  + "'" + label_name  + "'"   +
                                                              ",UPDT_TS=SYSDATE"            +
                                                              ",UPDT_USER_ID='BATCH'"         +
                                                          " WHERE NDC_NBR = " +  in_ndc_nbr);

                System.out.print("UPDATE IRX.IRX_DRUG_NM "               +
                                                          "SET DRUG_NM= "  + "'" + label_name  + "'"   +
                                                              ",UPDT_TS=SYSDATE"            +
                                                              ",UPDT_USER_ID='BATCH'"         +
                                                          " WHERE NDC_NBR = " +  in_ndc_nbr + "\n");
                wsDrugNameChange++;
          }
      }
      stmt2.close();
      stmt3.close();
      rset2.close();


 } catch (SQLException e) {
          e.printStackTrace();
         }



 }

public boolean DetermineIfDrugNamePresent (String in_ndc_nbr) {

 int rowNumber           = 0;
 Statement stmt2         = null;
 ResultSet rset2         = null;


  String SQL_stmt2 = "select 'YES' from irx.irx_drug_nm where " +
    " ndc_nbr =" + in_ndc_nbr;

try {
      stmt2 = con.createStatement();
      rset2 = stmt2.executeQuery(SQL_stmt2);

      rset2.next();
      rowNumber = rset2.getRow();

      stmt2.close();
      rset2.close();


 } catch (SQLException e) {
          e.printStackTrace();
         }


 if ( rowNumber > 0)
      {return true;}
  else
     {return false;}

}

public void MainProcess() {

            StringTokenizer st = null;

            String  Bus_unit;
            String  NDC_nbr;
            String  UPC_nbr;
            String  Ext_Date;
            String  Unit_price;
            String  Status;
            String  Label_name;

         try {


             System.out.print("Create FileReader Obj " + InputFileName + " \n");
             FileReader InputFileReader = new FileReader(InputFileName);
             System.out.print("Create BufferReader Obj \n");
             BufferedReader inputStream = new BufferedReader(InputFileReader);



             while ((inLine = inputStream.readLine()) != null) {
                 st = new StringTokenizer(inLine,DELIM);

                 Ext_Date = st.nextToken();

                 Bus_unit = st.nextToken();

                 NDC_nbr = st.nextToken();

                 UPC_nbr = st.nextToken();

                 Status  = st.nextToken();

                 Unit_price = st.nextToken();

                 Label_name = st.nextToken();


                  if (!DetermineIfPriceExist(NDC_nbr, Unit_price, Ext_Date, Label_name) ) {
                     InsertPricingRecord(Ext_Date, Bus_unit, NDC_nbr, UPC_nbr, Unit_price, Status); }
             }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.print(inLine + "\n");
            }


            try {
              con.commit();
            }  catch (SQLException e) {
               e.printStackTrace();
            }

  AutoCorrectIRX();
  System.out.print("Pricing Pgm inserted " + l_InsertCounter + " Records \n");
  System.out.print("Drug Name  inserted " + l_DrugNMCounter + " Records \n");
  System.out.print("Drug Name Description changes " + wsDrugNameChange + "\n");

}

public void AutoCorrectIRX() {

String SQL_stmt3="select SITE_ID, NDC_NBR, RX_PRSCRPTN_NBR,  RX_SEQ_NBR " +
 "from IRX_FILLS_ERRORS where error_code in (303, 302, 306) and status = 'E'";


int UpdatePriceResults = 0;
int DeleteErrorResults = 0;
long db_RX_PRSCRPTN_NBR = 0;
long db_NDC_NBR         = 0;
long db_SITE_ID         = 0;
long AutoCorrect_ct     = 0;
int db_RX_SEQ_NBR       = 0;
Statement stmt3         = null;
ResultSet rset3         = null;
Statement stmt5         = null;


 try {
      stmt3 = con.createStatement();
      stmt5 = con2.createStatement();
      rset3 = stmt3.executeQuery(SQL_stmt3);

      while (rset3.next()) {


            if ( DetermineIfPriceOnFile(rset3.getLong("NDC_NBR")) ) {

              db_SITE_ID         =  rset3.getLong("SITE_ID");
              db_NDC_NBR         =  rset3.getLong("NDC_NBR");
              db_RX_PRSCRPTN_NBR =  rset3.getLong("RX_PRSCRPTN_NBR");
              db_RX_SEQ_NBR      =  rset3.getInt("RX_SEQ_NBR");

              System.out.print("made it here " + rset3.getLong("NDC_NBR") + "\n");

              UpdatePriceResults = stmt5.executeUpdate("UPDATE IRX_FILLS "               +
                                                      "SET PROCESS_FLG='C' "             +
                                                           ",UPDT_DTTM=SYSDATE"          +
                                                           ",UPDT_USER_ID='AUTOCORRECT'" +
                                                      " WHERE RX_PRSCRPTN_NBR = " + rset3.getLong("RX_PRSCRPTN_NBR") +
                                                       " AND RX_SEQ_NBR = " + rset3.getInt("RX_SEQ_NBR") +
                                                       " AND NDC_NBR = " + rset3.getLong("NDC_NBR")     +
                                                       "AND SITE_ID = "  + rset3.getLong("SITE_ID"));

              System.out.print("RX_PRSCRPTN_NBR = " + rset3.getLong("RX_PRSCRPTN_NBR") +
                                                       " RX_SEQ_NBR = " + rset3.getInt("RX_SEQ_NBR") +
                                                       " NDC NBR = " +  rset3.getLong("NDC_NBR") + "\n");

              UpdatePriceResults = stmt5.executeUpdate("UPDATE IRX_FILLS_ERRORS "               +
                                                      "SET STATUS='C' "             +
                                                           ",UPDT_DTTM=SYSDATE"          +
                                                           ",UPDT_USER_ID='AUTOCORRECT'" +
                                                      " WHERE RX_PRSCRPTN_NBR = " + rset3.getLong("RX_PRSCRPTN_NBR") +
                                                       " AND RX_SEQ_NBR = " + rset3.getInt("RX_SEQ_NBR") +
                                                       " AND NDC_NBR = " + rset3.getLong("NDC_NBR")    +
                                                       " AND SITE_ID = "  + rset3.getLong("SITE_ID"));


/* --------------------------------------  Code change for IRX 2.0  ------------------------------------------
              DeleteErrorResults =  stmt5.executeUpdate("DELETE FROM IRX_FILLS_ERRORS "  +
                                                        "WHERE RX_PRSCRPTN_NBR = " + db_RX_PRSCRPTN_NBR +
                                                       " AND RX_SEQ_NBR = " + db_RX_SEQ_NBR +
                                                       " AND NDC_NBR = " + rset3.getLong("NDC_NBR"));
  -------------------------------------------------------------------------------------------------------------
 */
             AutoCorrect_ct++;


            }  //END-IF


      }   //end-while

      stmt3.close();
      stmt5.close();
      rset3.close();

      System.out.print("Auto Corrected " + AutoCorrect_ct + " \n");
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

public boolean DetermineIfPriceOnFile(long in_ndc) {
 String SQL_stmt4 = "Select distinct ndc_nbr from IRX_NDC_PRICE where NDC_NBR = " + in_ndc + " and MTRC_DECML_CST > 0";
 Statement stmt4         = null;
 ResultSet rset4         = null;
 boolean   YN_Flg        = false;


  try {
      stmt4 = con.createStatement();
      rset4 = stmt4.executeQuery(SQL_stmt4);
      rset4.next();
      if ( rset4.getRow() > 0 ) {
        System.out.print(" Price lookup successfull on NDC " + in_ndc + "\n");
        YN_Flg = true;
      } else {
          YN_Flg = false;
      }

    stmt4.close();
    rset4.close();


   } catch (SQLException sqle) {
           while (sqle != null) {
                 String logMessage = "\n SQL Error: " +  sqle.getMessage() + "\n\t\t"+
                 "Error code: "+sqle.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + sqle.getSQLState() + "\n";
                 System.out.println(logMessage);
                 sqle = sqle.getNextException();
           }

         }

 return YN_Flg;
}

public static void main(String[] args)
                throws java.lang.InterruptedException {

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
     System.out.print("IRX_pricingload Started " + Start_time.getTime() + "\n");
     IrxPricingLoad PL = new IrxPricingLoad();
     PL.MainProcess();
     Date End_time = new Date();
     elapsed_time = End_time.getTime() - Start_time.getTime();

     System.out.print("IRX_pricingload Processing Time " + elapsed_time / 1000 + " seconds \n");
     System.out.print("IRX_pricingload Ended "  + End_time.getTime() + "\n");

  }
}

