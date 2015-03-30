/*
 * IRX_AutoMedFileVerification.java VERSION 1.0
 *
 * Created on June 26, 2006, 1:32 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

import java.util.logging.*;
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

public class IRX_AutoMedFileVerification  {



 private static Logger logger = Logger.getLogger("IRX_Verification");

final  static String InputFileName = "/longs/irx/ftp/inbound/current_billing.txt";


    // runBox = GetHostName();




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



public static void main(String[] args)
                throws java.lang.InterruptedException {

 long  file_size = 0;
 Date  file_date = new Date();

         File f = new File( InputFileName );
                  if (f.exists()  && f.isFile()) {
                        lastModified = f.lastModified();

                         file_date =  new Date(lastModified);
                         file_size = f.length();
                   } else {

                     return (-1);

                   }
           if ( file_size == 0 ) {
            return (-1);
           }

}
