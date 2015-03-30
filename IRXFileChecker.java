/*
 * Main.java
 *
 * Created on June 26, 2007, 10:35 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
import java.util.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.DateFormat;
import java.lang.Class;


/**
 *
 * @author djefferson
 */
public class IRXFileChecker {
      String tbl_data [] = new String [50000];

      int line_ct        = 1;
      String IRXENV;
      String IRXDSN;
      FileReader InputFileReader ;
      BufferedReader inputStream ;
     public final static int GOOD      = 0;
     public final static int DUPLICATE = 1;
     public final static int EMPTY     = 3;
     public final static int NOFILLS   = 7;


    /** Creates a new instance of Main */
    public IRXFileChecker () {

    /**
     * @param args the command line arguments
     */
    }

 public int VerifyFileContent (String in_file_name) {

     int i_file_day_of_week     = 0;
     int i_file_RC              = 0;
     Date d_day                 =  new Date();
     Calendar c                 =  Calendar.getInstance();
     SimpleDateFormat formatter = new SimpleDateFormat("MMddyy");
     String str_File_Data []    = new String [300000] ;

     try {
       FileReader InputFileReader = new FileReader (in_file_name);
       while ((str_File_Data [line_ct] = inputStream.readLine()) != null) {
                                  line_ct++;
                            } // end-while for input-file reading
     } catch (IOException e) {
         e.printStackTrace();
     } 
       
       

     int i_current_day_of_week = c.get(Calendar.DAY_OF_WEEK);

     System.out.print("Today is " + i_current_day_of_week + "\n");

     if (in_file_name.length() > 20) {
           try {

              int pos = in_file_name.indexOf("Bills") + 5;

               Date filedate = formatter.parse(in_file_name.substring(pos , pos + 6));

              c.setTime(filedate);
              i_file_day_of_week = c.get(c.DAY_OF_WEEK);
              System.out.print("Input File Creation Date is " + c.DAY_OF_WEEK + "\n");

            }
              catch( ParseException e ) {

              System.out.print("Parse error on date \n");
               }


           if (i_current_day_of_week == i_file_day_of_week)   {

               switch (i_current_day_of_week) {

                   case 1:


               }

           }





         }



  return (i_file_RC);
 }
 public  int DetermineIfValid(String in_file_name, long in_hc, long in_hc_id, long in_len, String in_modTime ) {
         StringTokenizer st    = null;
         String s_hsh          = null;
         String s_hsh_id       = null;
         String s_filename     = null;
         String s_modTime      = null;
         String s_len          = null;
         int sub               = 1;
         String DELIM          = ",";

        while (sub < line_ct + 1  ) {
          st = new StringTokenizer(tbl_data[sub],DELIM);

            s_hsh      = st.nextToken();
            s_hsh_id   = st.nextToken();
            s_filename = st.nextToken();
            s_modTime  = st.nextToken();
            s_len      = st.nextToken();

            if (in_hc == Long.parseLong(s_hsh))  {
                return(1);
            }
            else
                sub = sub + 1;

        }    // End-while

 /* --------------------------------------------------------------------
  * Determine date of file along with header information, record type 4's
  * and trailer record
  *          RC = 0    All ok with file
  *          RC = 30   Missing header record
  *          RC = 31   Missing type 4 records
  *          RC = 32   Missing trailer record
  *----------------------------------------------------------------------
  */

     int i_File_RC = VerifyFileContent( in_file_name );

     if (i_File_RC > 0) {

         return (i_File_RC);
     }


     // Check for empty contents of file
     if ( Long.parseLong(s_len) == 0 ) {
         return (3);
     }


     return(0);
    }


 public void MainProcess () {
         long l_hc;
         long l_hc_id;
         long l_modTime ;
         long l_len;
         Date  s_FileCreate;
         String outputRecord = null;
         String str_env_file = null;


     try {

         IRXENV = System.getProperty("IRXENV");
         IRXDSN = System.getProperty("IRXDSN");

         System.out.print("IRX Property File is" + IRXENV + "\n");

         Properties p = new Properties ();
         p.load(new FileInputStream(IRXENV));

         str_env_file = p.getProperty("Registry");
         p.list(System.out);
     }
     catch (Exception e) {
         System.out.print ("IRX Env File not Found " + " \n");

         System.exit(7);
     }


        System.out.print("IRX input data file is " + IRXDSN + " \n");
                try {
                      boolean exists = (new File(IRXDSN)).exists();
                         if (exists) {
                             InputFileReader = new FileReader(str_env_file);

                             inputStream = new BufferedReader(InputFileReader);

                           while ((tbl_data [line_ct] = inputStream.readLine()) != null) {

                                  line_ct++;
                            } // end-while for input-file reading

                            line_ct = line_ct - 1;
                            inputStream.close();
                         } // End of IF Statement

                }
               catch ( IOException e) {
               e.printStackTrace();
               System.out.print ("Current IRX Input File not found \n");
               System.exit(11);
           } // End 1st catch stmt


         File file1 = new File(IRXDSN);
         l_modTime  = file1.lastModified();

         s_FileCreate = new Date (l_modTime);

         l_len      = file1.length();
         l_hc       = file1.hashCode() ;
         l_hc_id    = System.identityHashCode(file1);

         int i_return_code = DetermineIfValid(IRXDSN, l_hc, l_hc_id, l_len, s_FileCreate.toString());
         System.out.print("interal return code is " + i_return_code + "\n");

           switch (i_return_code ) {
            case DUPLICATE :

               System.out.print("Error *** Duplicate file found ** \n");
               System.out.print ("Filename is " + IRXDSN  + " \n");
               System.out.print("File creation Date is " + s_FileCreate + "\n");
               System.out.print ("Hash Code is " + l_hc  + " \n");
               System.out.print ("Hash Code Id is " + l_hc_id  + " \n");
               System.out.print ("File Size is  " + l_len  + " \n");
               System.out.print("Program will exit with return code of " + DUPLICATE +  "\n");
               System.exit(1);
            case EMPTY :

              System.out.print("Error *** Empty AutoMed File ** \n");
              System.out.print ("Filename is " + IRXDSN  + " \n");
              System.out.print("File creation Date is " + s_FileCreate + "\n");
              System.out.print ("Hash Code is " + l_hc  + " \n");
              System.out.print ("Hash Code Id is " + l_hc_id  + " \n");
              System.out.print ("File Size is  " + l_len  + " \n");
              System.out.print("Program will exit with return code of " + EMPTY +  "\n");
              System.exit(3);
           case GOOD :

                try {
                      PrintWriter outputStream = null;
                      outputStream = new PrintWriter (new FileWriter(str_env_file));

                      for (int i=1; i < line_ct + 1 ; i++) {
                           outputStream.println (tbl_data[i]);
                      } // end-for loop

                      outputRecord =  l_hc + "," + l_hc_id + "," + IRXDSN + "," + s_FileCreate + "," + l_len;
                      outputStream.println(outputRecord);
                      outputStream.close();
                      System.exit(GOOD);

                } catch (IOException e) {
                         System.out.print("I/O Error on writing file retention file \n");
                         System.exit(13);
                 }  //  End - Catch
           }    // End - Case
 }   // End - Main

    public static  void main(String[] args) throws java.lang.InterruptedException {

        IRXFileChecker mp =  new IRXFileChecker();
        mp.MainProcess();
    }


}
