

//
/*
 * apilfixer.java
 *
 * Created on September 12, 2007, 12:12 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
import java.io.*;
import java.sql.*;
import java.util.*;
import java.lang.*;
import java.text.*;
import java.net.*;

/**
 *
 * @author consepio
 */
public class apilfixer {
    String  Tbl_Data []  = new String[6000];
    int ws_idx = 0;
    Connection ConnectionURL;
    String INFORMX_URL="jdbc:informix-sqli://uxp.longs.com:6243/apmdb:INFORMIXSERVER=apx0001";
    //String INFORMX_URL = "jdbc:odbc:apm70hp2";

    String INFORMX_USERID="psfsadm";
    String INFORMX_USERPASS="load7499";

    /** Creates a new instance of apilfixer */
    public apilfixer() {

   try {
        System.out.print("Connection to Informix Driver \n");
        Class.forName("com.informix.jdbc.IfxDriver");
       // Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
        ConnectionURL = DriverManager.getConnection(INFORMX_URL, INFORMX_USERID,INFORMX_USERPASS);
    }  catch (Exception e) {
             System.err.println( "Unable to locate class \n" );
             e.printStackTrace();
             System.exit(1);
       }


} // End of Constructor apilfixer

public void GetFileDates (long in_start_date, long in_end_date) {




} // End of GetFileDates

public String ConvertStore(int in_str) {

  NumberFormat formatter = new DecimalFormat("000");
  String s = formatter.format(in_str);


  return s;
}

public void ProcessFile(String in_date) {

    for (int i=2; i<999; i++) {

           String ws_store = ConvertStore(i);

           String str_file_name = "c:\\apil\\apmapil-" + ws_store + "_2007" + in_date + ".html";
           System.out.print("Process file" + str_file_name + "\n");

           File f = new File( str_file_name );

           if (f.exists()) {
                try {
                   // str_file_name = "c:\\temp\\apmapil-" + "002" + "_2007" + "0907" + ".html";
                    BufferedReader in = new BufferedReader(new FileReader(str_file_name));
                    String str;
                     ws_idx = 0;

                     while ((str = in.readLine()) != null) {
                             Tbl_Data[ws_idx] = str;
                             ws_idx++;
                    } // End While
                    in.close();
                    ScanData(ws_idx, str_file_name);
                } catch (IOException e) {
                     System.out.print("File error \n");
                  } // End Catch

           } // End If-Stmt
    } // End For-LOOP


} // end ProcessFile

public void ScanData (int in_idx, String in_file_name) {
  String str_ref="<a href='http://uxp.longs.com/coldfusion_source/BOL_search_v_module7.cfm?bol_nbr='";
  String str_BOL = null;
  String str = null;
  boolean b_Need_Bol   = false;
  int pos              = 0;
  int len              = 0;
  int pos_claim        = 0;
  String str_claim_nbr = null;
  String sClaim_Nbr     = null;

  for (int i=0; i < in_idx; i++) {

       pos = Tbl_Data[i].indexOf("                        678-");

      if (pos > 0) {
           pos_claim = Tbl_Data[i].indexOf("678-");
           str_claim_nbr = Tbl_Data[i].substring(pos_claim, pos_claim+9);
           sClaim_Nbr = str_claim_nbr.replace("-","");

           System.out.print(sClaim_Nbr + "\n");

           b_Need_Bol = true;
           len = Tbl_Data[i].length();
           str = Tbl_Data[i].substring(45,69);
           str = str.replace("             ",str_ref);
           str_BOL = GetBolNumber(sClaim_Nbr);
           str = str.replace("bol_nbr=", "bol_nbr=" + str_BOL);
           Tbl_Data[i] = Tbl_Data[i].replace("                        " + str_claim_nbr, "    " + str  + ">" + str_BOL + "</a>" + "    " + "<a href='http://uxp.longs.com/coldfusion_source/BOL_search_v_module7.cfm?claim_nbr='"  + ">" + str_claim_nbr + "</a>");
           Tbl_Data[i] = Tbl_Data[i].replace("?claim_nbr=", "?claim_nbr=" + sClaim_Nbr);
           System.out.print(Tbl_Data[i]  + "\n");
      } // END-IF


  } // End FOR LOOP

     if (b_Need_Bol) {
       WriteOutputFile(in_idx,in_file_name);

   }



}

public String GetBolNumber(String in_claim_nbr) {

    String SQL1="select bol_nbr from enhanced_bol where claim_nbr=" + in_claim_nbr;
    // String ws_BOL_nbr = "0873000005000799";
    double ws_BOL_nbr = 0;
    String str_BOL_nbr = null;
    Statement stmt1 = null;

    // System.out.print("incoming claim " + in_claim_nbr + "\n");
    try {
        // Create a result set containing all data from my_table
        stmt1 = ConnectionURL.createStatement();
        ResultSet rs = stmt1.executeQuery(SQL1);

        rs.next();
        str_BOL_nbr = rs.getString(1);
        System.out.print("From Database " + ws_BOL_nbr + "\n");
        rs.close();
    } catch (SQLException e) {
         e.printStackTrace();
           while (e != null) {
                 String logMessage = "\n SQL Error: " +  e.getMessage() + "\n\t\t"+
                 "Error code: "+e.getErrorCode() +  "\n\t\t"+
                 "SQLState: " + e.getSQLState() + "\n";
                 System.out.println(logMessage);
                 e = e.getNextException();
           }

         System.exit(1);
    }



return str_BOL_nbr;
}
public void WriteOutputFile (int in_idx, String in_file_name) {

       String str_file_name = in_file_name.replace(".html", "_adj.html");
       String outfilename = str_file_name ;

       try {
        BufferedWriter out = new BufferedWriter(new FileWriter(outfilename));
        System.out.print(outfilename + "\n");
        for (int i=0; i < in_idx; i++) {
             out.write(Tbl_Data[i] + "\n");
        }
        out.close();
    } catch (IOException e) {
        e.printStackTrace();

    }




}
public void MainLogic () {
    long  ws_start_date = 907;
    long  ws_end_date   = 0;

    GetFileDates(ws_start_date, ws_end_date);

    System.out.print("Processing start \n");
 //   while (ws_start_date != ws_end_date) {
          System.out.print("Processing Date ws_start_date \n");

          ProcessFile("0907");



  //  }
}
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
      apilfixer AF = new apilfixer();
      AF.MainLogic();
    }

}

