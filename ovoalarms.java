import javax.xml.parsers.*;
import java.io.*;
import java.text.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Time;



import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class OVOProcess {

	public static String run_date;
	public static String timestamp;
	public static String ecn;
	public static String xaid;
	public static String request_id;
	public static String  fault_code;
	public static String  severity;
	public static String  module;
	public static String  advice_txt;
	public static String  business_msg;
	public static String  detail_item_code;
	public static String  fault_reason_txt;
	public static String  fault_string;
	public static String  technicalMessage;



	private static final String fname = "c:\\tmp\\ovo_alarms_bankers.log";
	//private static final String fname = "c:\\temp\\ovo_banker.log";
	public static int ct = 0;
	private static Connection connect = null;
	private static Statement statement = null;
	private static ResultSet resultSet = null;


	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

	   BuildJavaDB();
	   String line = null;
		FileInputStream fis;
		try {
			fis = new FileInputStream(fname);

			System.out.println("Total file size to read (in bytes) : "
					+ fis.available());

			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

			while ((line = reader.readLine()) != null) {

                if ((line.length() > 1000) && line.indexOf("<WFFault>") > 0 && line.indexOf("</WFFault>") > 0)
                	{
                	StripHeaderLine(line);
                	String xml_line = GetOnlyXMLString( line);
                	ProcessCurrentLine(xml_line);
                	}
                else
                	ProcessNonXmlLine(line);

				//Document doc = loadXMLFromString(line);

			}
			 System.out.println("Number of rows insert is : " + ct);
		} catch (IOException e) {
			e.printStackTrace();

		}
       DumpJavaDB();

	}

	public static void BuildJavaDB() throws  InstantiationException, IllegalAccessException, ClassNotFoundException
	   {
		   try {

			    String driver = "org.apache.derby.jdbc.EmbeddedDriver";
			    String dbName = "ovo";
			    String connectionURL = "jdbc:derby:memory:" + dbName + ";create=true";

			    String createString = "CREATE TABLE OVOALARM (rundate DATE  ," +
			    		              " systime time , " +
			    		              " ecn VARCHAR(15) , " +
			    		              " xaid VARCHAR(15) , " +
			    		              " request_id INT , " +
			    		              " module VARCHAR(150) , " +
			    		              " severity VARCHAR(55) , " +
			    		              " fault_reason_txt VARCHAR(65), " +
			    		              " fault_code VARCHAR(65) , " +
			    		              " fault_string VARCHAR(65) , " +
			    		              " advice_txt VARCHAR(300) , " +
			    		              " business_msg VARCHAR(500) , " +
			    		              " detail_item_code VARCHAR(300)," +
			    		              " technical_msg VARCHAR(80) )";

			    Class.forName(driver);

			    connect = DriverManager.getConnection(connectionURL);

			    Statement stmt = connect.createStatement();

			    stmt.executeUpdate(createString);
			    System.out.println("Success in create javadb");
			    }

			    catch (SQLException e)
			    {
			    	System.out.println (e.getMessage());
			    }



	   }

	public static void DumpJavaDB() throws SQLException
	{

			File file = new File("c:\\temp\\output.csv");
			try {
				PrintWriter pw = new PrintWriter(file);
				
				pw.println("rundate;" +
	              "  time ; " +
	              " ecn ; " +
	              " xaid ; " +
	              " request_id ; " +
	              " module; " +
	              " severity; " +
	              " fault_reason_txt;" +
	              " fault_code; " +
	              " fault_string ; " +
	              " advice_txt; " +
	              " business_msg ; " +
	              " detail_item_code ; " +
	              " technical Message");
				
				
			    Statement stmt2 = connect.createStatement();
			    ResultSet resultSet = stmt2.executeQuery("select * from OVOALARM order by rundate");
			    System.out.println("Dump OVO Alarm Table for Reporting \n\n");
			    int num = 0;

			    while (resultSet.next()) {
			      System.out.println(resultSet.getDate(1) +  ";" + resultSet.getString(2) +  ";" + resultSet.getString(3)
			    		  +  ";" + resultSet.getString(4) +  ";" + resultSet.getString(5) +  ";" + resultSet.getString(6) 
			    		  +  ";" + resultSet.getString(7) +  ";" + resultSet.getString(8) +  ";" + resultSet.getString(9) 
			    		  +  ";" + resultSet.getString(10) +  ";" + resultSet.getString(11) +  ";" + resultSet.getString(12) 
			    		  +  ";" + resultSet.getString(13) +  ";" + resultSet.getString(14));

			      pw.println(resultSet.getDate(1) +  ";" + resultSet.getString(2) +  ";" + resultSet.getString(3)
			    		  +  ";" + resultSet.getString(4) +  ";" + resultSet.getString(5) +  ";" + resultSet.getString(6) 
			    		  +  ";" + resultSet.getString(7) +  ";" + resultSet.getString(8) +  ";" + resultSet.getString(9) 
			    		  +  ";" + resultSet.getString(10) +  ";" + resultSet.getString(11) +  ";" + resultSet.getString(12)
			    		  +  ";" + resultSet.getString(13) + ";" + resultSet.getString(14));
			      
			    }
			    resultSet.close();
				
				pw.close();
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		    
		    
		    System.out.println("Finished dumping table " + ct);
	}


  public static void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }

	   public static void StripHeaderLine( String line) throws SQLException
	   {


		   String [] line_elements = line.split(" ");
		    run_date = line_elements [0];
			timestamp = line_elements [1];
 
			System.out.println(line_elements[8].toString());
			if (line_elements[8].toString().indexOf("REQUEST_AUDIT.request_ID=") == 0)
			{
				Pattern pattern = Pattern.compile("([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])");
				Matcher matcher = pattern.matcher(line_elements[8].toString());
				if (matcher.find())
				{
				    //System.out.println(matcher.group(0));
				    request_id =  matcher.group(0);
				}
				
				Pattern pattern2 = Pattern.compile("([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])");
				Matcher matcher2 = pattern2.matcher(line_elements[10].toString());
				if (matcher2.find())
				{
				    //System.out.println(matcher2.group(0));
				    ecn =  matcher2.group(0);
				}
				
				Pattern pattern3 = Pattern.compile("([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])");
				Matcher matcher3 = pattern3.matcher(line_elements[11].toString());
				if (matcher3.find())
				{
				    //System.out.println(matcher3.group(0));
				    xaid =  matcher3.group(0);
				}
				
				Pattern pattern4 = Pattern.compile("(com.*)");
				Matcher matcher4 = pattern4.matcher(line_elements[11].toString());
				if (matcher4.find())
				{
				    //System.out.println(matcher4.group(0));
				    module =  matcher4.group(0);
				}
				
				severity = line_elements[3].toString();
				fault_string = line_elements[9].toString();
				
				fault_reason_txt = " ";
				fault_code = " ";
				advice_txt = " ";
				business_msg = " ";
				detail_item_code = " ";
				

			}
			else
			{
				severity = " ";
				fault_string = " ";
				ecn = " ";
				xaid = " ";
				request_id=" ";
				module = " ";
				fault_reason_txt = " ";
				fault_code = " ";
				advice_txt = " ";
				business_msg = " ";
				detail_item_code = " ";
				
			}
	   }

	   public static Document loadXMLFromString(String xml) throws Exception
	    {
	        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder builder = factory.newDocumentBuilder();
	        InputSource is = new InputSource(new StringReader(xml));
	        return builder.parse(is);
	    }



	public static void ProcessCurrentLine (String xml_line) throws SQLException
	 {
		 try
		 {
			//System.out.println(line);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			InputSource is = new InputSource(new StringReader(xml_line));
			Document doc = dBuilder.parse(is);

			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();

			//System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

			NodeList nList = doc.getElementsByTagName("WFFault");

			//System.out.println("----------------------------");

			for (int index = 0; index < nList.getLength(); index++) {

				//Node nNode = nList.item(temp);
				Element element = (Element) nList.item(index);


				//System.out.println("\nCurrent Element :" + element.getNodeName());
				//System.out.println(ct++);

				if (element.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) element;


					//System.out.println("Staff id : " + eElement.getAttribute("id"));
					fault_code = eElement.getElementsByTagName("faultCode").item(0).getTextContent();
					severity = eElement.getElementsByTagName("severity").item(0).getTextContent();

					if (eElement.getElementsByTagName("businessMessage").item(0) != null) {
						business_msg =  eElement.getElementsByTagName("businessMessage").item(0).getTextContent();
					}
					else
					{ business_msg = " "; }
					
					
					if (eElement.getElementsByTagName("detailItemCode").item(0) != null) {
						detail_item_code =  eElement.getElementsByTagName("detailItemCode").item(0).getTextContent();
					} 
					else { detail_item_code = " "; }

					if (eElement.getElementsByTagName("faultString").item(0) != null) {
						fault_string = eElement.getElementsByTagName("faultString").item(0).getTextContent();
					}
					else { fault_string = " ";}

					if (eElement.getElementsByTagName("adviceText").item(0) != null) {
						advice_txt = eElement.getElementsByTagName("adviceText").item(0).getTextContent();
					}
					else { advice_txt = " ";}

					if (eElement.getElementsByTagName("technicalMessage").item(0) != null) {
						technicalMessage = eElement.getElementsByTagName("technicalMessage").item(0).getTextContent();
					}
					else { technicalMessage = " ";}

				    PreparedStatement psInsert = connect
					        .prepareStatement("insert into OVOALARM values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

				    psInsert.setString(1, run_date);
				    psInsert.setString(2, timestamp);
				    psInsert.setString(3, ecn);
				    psInsert.setString(4, xaid);
				    psInsert.setString(5, request_id);
				    psInsert.setString(6, module);
				    psInsert.setString(7, severity);
				    psInsert.setString(8, fault_reason_txt);
				    psInsert.setString(9, fault_code );
				    psInsert.setString(10, fault_string);
				    psInsert.setString(11, advice_txt);
				    psInsert.setString(12, business_msg);
				    psInsert.setString(13, detail_item_code);
				    psInsert.setString(14, technicalMessage);

					psInsert.executeUpdate();
					ct++;
				}
			}
		 }
          catch (ParserConfigurationException e) {
        	  e.printStackTrace();
        	  e.getMessage();
          }
	       catch (SAXException e) {
		    e.printStackTrace(); }
		  catch (IOException e) {
		    e.printStackTrace(); }

	 }

	public static void ProcessNonXmlLine (String line)
	{
		String  [] Ovo_items;

		Ovo_items = line.split("\\s+");

		if (line.indexOf("com.wellsfargo.isg.domain.model.account.AccountNotFoundException:") > 0)
		{

			System.out.println(Ovo_items[0]);
		}
		else
		if (line.indexOf("com.wellsfargo.isg.domain.model.billing.BillingException") > 0)
		{
			System.out.println(Ovo_items[0]);
		}


	}
	public static String GetOnlyXMLString(String line)
	{
		int startpos;
		int endpos;

		System.out.println(line);
		startpos = line.indexOf("<WFFault>")  ;
		endpos = line.indexOf("</WFFault>");
		line = line.substring(startpos, endpos+10);
		line.replace('|', ' ');

		return line;
	}

	 }

