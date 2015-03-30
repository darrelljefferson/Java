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



public class IRX_DrugNm_Load {

    private String sNDC_ID;
    final  static String driverClass   = "oracle.jdbc.driver.OracleDriver";
    final  static String connectionTEST = "jdbc:oracle:thin:@uxn.longs.com:1521:FDMDEV01";
    final  static String connectionPROD = "jdbc:oracle:thin:@ux14.longs.com:1521:FDMPRD01";
           static String connectionURL;
    final  static String userID        = "irxuser";
    final  static String userPassword  = "irxuser";
    final  static String InputFileName = "/longs/irx/ftp/inbound/go_live_drug_names.txt";
    final  static String DELIM         = ";";
    Connection con                     =   null;
    Connection con2                    =   null;
    long lndc_nbr;
    String inLine                      = null;
    long l_InsertCounter;
    public static String str_Env;


    /** Creates a new instance of IrxPricingLoad */
public IRX_DrugNm_Load() {
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
            this.con2 = DriverManager.getConnection(connectionURL, userID, userPassword);
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


public void InsertPricingRecord( String In_ndc_nbr, String In_drug_nm) {


 Statement stmt   =   null;
 int InsertPriceResults = 0;
 String  Bus_Unit;
 String  str_ext_date = null;


     try {
          stmt = con.createStatement ();
          lndc_nbr = Long.parseLong(In_ndc_nbr);


          InsertPriceResults = stmt.executeUpdate(
                      "INSERT INTO IRX.IRX_DRUG_NM " +
                       "(NDC_NBR,"              +
                       "DRUG_NM,"               +
                       "UPDT_TS,"               +
                       "UPDT_USER_ID,"          +
                       "CREATE_TS,"             +
                       "CREATE_USER_ID)"        +
                        " VALUES ("             +
                        lndc_nbr                + ",'"   +
                        In_drug_nm              + "',"   +
                        "SYSDATE"               + ","   +
                        "'CONVERSION'"          + ","   +
                        "SYSDATE"               + ","   +
                        "'CONVERSION')"  );


          l_InsertCounter++;

          stmt.close();

        } catch (SQLException e) {
             e.printStackTrace();

                       System.out.print("INSERT INTO IRX.IRX_DRUG_NM " +
                       "(NDC_NBR,"              +
                       "DRUG_NM,"               +
                       "UPDT_TS,"               +
                       "UPDT_USER_ID,"          +
                       "CREATE_TS,"             +
                       "CREATE_USER_ID)"        +
                        " VALUES ("             +
                        lndc_nbr                + ","   +
                        In_drug_nm              + ","   +
                        "SYSDATE"               + ","   +
                        "'CONVERSION'"          + ","   +
                        "SYSDATE"               + ","   +
                        "'CONVERSION')" + "\n" );


             System.out.print ( "ERROR ---> INSERT FAILED FOR NDC PRICE ->  " + lndc_nbr + " Drug name --> " + In_drug_nm + "\n" );
        }
    }
public boolean DetermineIfPriceExist(String in_ndc_nbr) {

 int rowNumber           = 0;
 Statement stmt2         = null;
 ResultSet rset2         = null;




 String SQL_stmt2 = "select ndc_nbr from irx.irx_drug_nm where " +
      " NDC_NBR="  + in_ndc_nbr;

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


 if (( rowNumber > 0  ))
      {return true;}
  else
     {return false;}

}

/*--------------------------------------------------------------------------------*
 *                                                                                *
 *                                                                                *
 *--------------------------------------------------------------------------------*/
public String GetHostName () {
  String hostname;
  InetAddress addr = null;

//    try {

        // Get the host name
        hostname = addr.getHostName();
        System.out.print("Running on " + hostname + " \n");
//    }
//    catch (UnknownHostException e) {
//    }

   return hostname;
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

                 NDC_nbr = st.nextToken();

                 Label_name = st.nextToken();

                  if (!DetermineIfPriceExist(NDC_nbr) ) {
                     InsertPricingRecord( NDC_nbr, Label_name); }
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


     System.out.print("Total Number of recs inserted is " +  l_InsertCounter + "\n");
}


public boolean DetermineIfPriceOnFile(long in_ndc) {
 String SQL_stmt4 = "Select 'YES' from IRX.IRX_DRUG_NM where NDC_NBR = " + in_ndc ;
 Statement stmt4         = null;
 ResultSet rset4         = null;
 boolean   YN_Flg        = false;

  try {
      stmt4 = con.createStatement();
      rset4 = stmt4.executeQuery(SQL_stmt4);
      rset4.next();
      System.out.print(" Price lookup " + rset4.getString(1) + "\n");

      if ( rset4.isFirst() ) {
        YN_Flg = true;
      } else {
         YN_Flg = false;
       }
    stmt4.close();
    rset4.close();


   } catch (SQLException e) {
          e.printStackTrace();
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
     IRX_DrugNm_Load PL = new IRX_DrugNm_Load();
     PL.MainProcess();
     Date End_time = new Date();
     elapsed_time = End_time.getTime() - Start_time.getTime();

     System.out.print("IRX_pricingload Processing Time " + elapsed_time / 1000 + " seconds \n");
     System.out.print("IRX_pricingload Ended "  + End_time.getTime() + "\n");

  }
}
