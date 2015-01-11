package edu.brown.cs.cs127.etl.importer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import au.com.bytecode.opencsv.*; 
import java.util.*;
import java.text.*;
import java.io.*;

public class EtlImporter
{
    private Connection conn;

    public EtlImporter(String pathToDatabase) throws Exception
    {
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

        Statement stat = conn.createStatement();
        stat.executeUpdate("PRAGMA foreign_keys = ON;");

        stat.executeUpdate("PRAGMA synchronous = OFF;");
        stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
        
        stat.executeUpdate("DROP TABLE IF EXISTS Airport;");
        stat.executeUpdate("DROP TABLE IF EXISTS Airline;");
        stat.executeUpdate("DROP TABLE IF EXISTS Flight;");
    }
    
    public void creat_airline_table(String AIRLINES_FILE) throws SQLException,FileNotFoundException,IOException
    {
        CSVReader reader = new CSVReader(new FileReader(AIRLINES_FILE));
         String [] nextLine;
         
         String query =
                "CREATE TABLE Airline" +
                 "(" +
                 "airline_code varchar(10) NOT NULL PRIMARY KEY," +
                 "airline_name varchar(70) NOT NULL" +
                 ");"; 
         PreparedStatement prep = conn.prepareStatement(query);
         prep.executeUpdate();
         
         query="INSERT OR IGNORE INTO Airline" +
                 " VALUES (?, ?);";
         prep = conn.prepareStatement(query);
         while ((nextLine = reader.readNext()) != null) {
            prep.setString(1, nextLine[0]);
            prep.setString(2, nextLine[1]);
            prep.addBatch();
         }
           conn.setAutoCommit(false);
           prep.executeBatch();
           conn.setAutoCommit(true);
           reader.close();
    }
    
    public void creat_airport_table(String AIRPORTS_FILE) throws SQLException,FileNotFoundException,IOException
    {
        CSVReader reader = new CSVReader(new FileReader(AIRPORTS_FILE));
         String [] nextLine;
         
         String query =
                "CREATE TABLE Airport" +
                 "(" +
                 "airport_code varchar(10) NOT NULL PRIMARY KEY," +
                 "airport_name varchar(50) NOT NULL," +
                 "city varchar(30)," +
                 "state varchar(15)" +
                 ");"; 
         PreparedStatement prep = conn.prepareStatement(query);
         prep.executeUpdate();
         
         query="INSERT OR IGNORE INTO Airport" +
                 " VALUES(?,?,?,?);";
         prep = conn.prepareStatement(query);
         while ((nextLine = reader.readNext()) != null) {
            prep.setString(1, nextLine[0]);
            prep.setString(2, nextLine[1]);
            prep.setString(3, "NULL");
            prep.setString(4, "NULL");
            prep.addBatch();
         }
           conn.setAutoCommit(false);
           prep.executeBatch();
           conn.setAutoCommit(true);
           reader.close();
    }
    
    public void creat_flight_table(String FLIGHTS_FILE) throws SQLException,FileNotFoundException,IOException,ParseException
    {
        CSVReader reader = new CSVReader(new FileReader(FLIGHTS_FILE));
        String [] nextLine;
         
        String query =
                "CREATE TABLE Flight" +
                 "(" +
                 "ID int PRIMARY KEY," +
                 "flight_num int," +
                 "airline_code varchar(10) NOT NULL," +
                 "originating_airport_code varchar(10)," +
                 "destination_airport_code varchar(10)," +
                 "depart_date varchar(20) NOT NULL," +
                 "depart_time int," +
                 "arrive_date varchar(20) NOT NULL Check (arrive_date>depart_date)," +
                 "arrive_time int," +
                 "cancelled bit NOT NULL," +
                 "carrier_delay int," +
                 "weather_delay int," +
                 "air_traffic_delay int," +
                 "security_concern_delay int," +
                 "FOREIGN KEY (airline_code) REFERENCES Airline(airline_code)," +    
                 "FOREIGN KEY (originating_airport_code) REFERENCES Airport(airport_code)," + 
                 "FOREIGN KEY (destination_airport_code) REFERENCES Airport(airport_code));"; 
         PreparedStatement prep = conn.prepareStatement(query);
         prep.executeUpdate();
         
         query="UPDATE Airport" +
                 " SET city=?,state=?" +
                 " WHERE airport_code=? or airport_code=?;";
         
         String query1="INSERT OR IGNORE INTO Flight" +
                 " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
         PreparedStatement  prep1 = conn.prepareStatement(query1);
         prep = conn.prepareStatement(query);
         int ID=0;
         
            String query2=
            "Select airline_code from Airline " +
                    "Where airline_code=?;";
            String query3=
            "Select airport_code from Airport " +
                    "Where airport_code=? or airport_code=?;";

            PreparedStatement prep2 = conn.prepareStatement(query2);
            PreparedStatement prep3 = conn.prepareStatement(query3);
            
            
         while ((nextLine = reader.readNext()) != null) {
            //-----------------------begin processing date-----------------------
            String arrivetime=nextLine[11]+" "+nextLine[12];//arrive time
            String departtime=nextLine[8]+" "+nextLine[9];//depart time
            Date departdate1=new Date();
            Date arrivedate1=new Date();
            String[] type={"yyyy-MM-dd hh:mm a","yyyy/MM/dd hh:mm a",
                    "yyyy-MM-dd HH:mm","yyyy/MM/dd HH:mm"};
            String[]type1={"MM/dd/yyyy hh:mm a","MM-dd-yyyy hh:mm a",
            		"MM/dd/yyyy HH:mm","MM-dd-yyyy HH:mm"};
            for(int i=0;i<type.length;i++)
            {
            	if((departtime.charAt(2)=='-')||(departtime.charAt(2)=='/'))
            	{
            		SimpleDateFormat df = new SimpleDateFormat(type1[i], Locale.US);
            		try{
            			departdate1 = df.parse(departtime);
            			break;
            			}
                catch(ParseException e){
                }
            		}
            	else
            	{
            		SimpleDateFormat df = new SimpleDateFormat(type[i], Locale.US);
            		try{
            			departdate1 = df.parse(departtime);
            			break;
            			}
                catch(ParseException e){
                }
            	}
            }
            
            for(int i=0;i<type.length;i++)
            {
            	if((arrivetime.charAt(2)=='-')||(arrivetime.charAt(2)=='/'))
            	{
            		SimpleDateFormat df1 = new SimpleDateFormat(type1[i], Locale.US);
            		try{
            			arrivedate1 = df1.parse(arrivetime);
            			break;
            			}
                catch(ParseException e){
                }
            		}
            	else
            	{
            		SimpleDateFormat df1 = new SimpleDateFormat(type[i], Locale.US);
            		try{
            			arrivedate1 = df1.parse(arrivetime);
            			break;
            			}
                catch(ParseException e){
                }
            	}
            }
            
            SimpleDateFormat df3 = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );
            String departdate = df3.format(departdate1);  
            String arrivedate = df3.format(arrivedate1);
            //-----------------------end processing date-----------------------
            
            if(arrivedate.compareTo(departdate) < 0)
                continue;
            //couldn't arrive before depart
            
            Calendar cal = Calendar.getInstance();
            Calendar cal1 = Calendar.getInstance();
            cal.setTime(departdate1);
            cal1.setTime(arrivedate1);
            cal.add(Calendar.MINUTE,Integer.parseInt(nextLine[10]));
            //add the minutes between actual and schedule depart time
            cal1.add(Calendar.MINUTE,Integer.parseInt(nextLine[13]));
            //add the minutes between actual and schedule arrive time
            String actualdeparttime_plus_delay = df3.format(cal.getTime());
            String actualarrivetime = df3.format(cal1.getTime());
            if(actualarrivetime.compareTo(actualdeparttime_plus_delay)<0)
                continue;
            //the real depart time plus the total delay time couldn't
            //be bigger than 
/*            if( (Integer.parseInt(nextLine[14])!=0) && //cancelled=1
                    ((Integer.parseInt(nextLine[10])!=0) //depart_time
                            ||(Integer.parseInt(nextLine[13]) !=0) //arrive_time
                            ||(Integer.parseInt(nextLine[15]) !=0)//carrier_delay
                            ||(Integer.parseInt(nextLine[16]) !=0)//weather_delay
                            ||(Integer.parseInt(nextLine[17]) !=0) //traffic_delay
                            ||(Integer.parseInt(nextLine[18]) !=0)//security_delay
                            ))
                continue;
            //if cancelled,all these items should be zeros
*/            
            if((Integer.parseInt(nextLine[15])<0)//carrier_delay
                  ||(Integer.parseInt(nextLine[16])<0)//weather_delay
                  ||(Integer.parseInt(nextLine[17])<0) //traffic_delay
                  ||(Integer.parseInt(nextLine[18])<0)//security_delay
                            )
                continue;
            
            
            prep2.setString(1,nextLine[0]);
            ResultSet rs = prep2.executeQuery();
            prep3.setString(1,nextLine[2]);
            prep3.setString(2,nextLine[5]);
            ResultSet rs1 = prep3.executeQuery();
            if((rs.next()==false) || (rs1.next()==false))
            {//airline_code cannot be found or destination_airport or 
             //originating_airport can not be found
                rs.close();
                rs1.close();
                continue;
            }
            if(rs1.next()==false)
            {
                rs.close();
                rs1.close();
                continue;
            }
            
            boolean cancel=(Integer.parseInt(nextLine[14])==1);
                
                
            prep1.setInt(1,ID);//ID
            prep1.setInt(2,Integer.parseInt(nextLine[1]));//flight_num
            prep1.setString(3, nextLine[0]);//airline_code
            prep1.setString(4, nextLine[2]);//originating_airport_code
            prep1.setString(5, nextLine[5]);//destination_airport_code
            prep1.setString(6, departdate);//depart_date
            prep1.setInt(7, Integer.parseInt(nextLine[10]));//depart_time
            prep1.setString(8, arrivedate);//arrive_date
            prep1.setInt(9, Integer.parseInt(nextLine[13]));//arrive_time
            prep1.setBoolean(10,cancel);//cancelled
            prep1.setInt(11, Integer.parseInt(nextLine[15]));//carrier_delay
            prep1.setInt(12, Integer.parseInt(nextLine[16]));//weather_delay
            prep1.setInt(13, Integer.parseInt(nextLine[17]));//air_traffic_delay
            prep1.setInt(14, Integer.parseInt(nextLine[18]));//security_concern_delay
            prep1.addBatch();
            //query1="INSERT OR IGNORE INTO table Flight" +
             //" (?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
            prep.setString(1,nextLine[3]);//city
            prep.setString(2,nextLine[4]);//state
            prep.setString(3,nextLine[2]);//originating_airport_code
            //insert into the Airport table the according city and state
            //query="UPDATE table Airport" +
            //" SET city=?,state=?" +
            //" WHERE airport_code=?;";
            prep.addBatch();
            prep.setString(1,nextLine[6]);//city
            prep.setString(2,nextLine[7]);//state
            prep.setString(3,nextLine[5]);//destination_airport_code
            prep.addBatch();
            ID++;
         }
           conn.setAutoCommit(false);
           prep.executeBatch();
           prep1.executeBatch();
           conn.setAutoCommit(true);
           reader.close();
    }
    
    /**
     * You are only provided with a main method, but you may create as many
     * new methods, other classes, etc as you want: just be sure that your
     * application is runnable using the correct shell scripts.
     */

    public static void main(String[] args) throws Exception
    {
        if (args.length != 4)
        {
            System.err.println("This application requires exactly four parameters: " +
                    "the path to the airports CSV, the path to the airlines CSV, " +
                    "the path to the flights CSV, and the full path where you would " +
                    "like the new SQLite database to be written to.");
            System.exit(1);
        }

        String AIRPORTS_FILE = args[0];
        String AIRLINES_FILE = args[1];
        String FLIGHTS_FILE = args[2];
        String DB_FILE = args[3];
/*
        String AIRPORTS_FILE = "/Users/woody/airports.csv";
        String AIRLINES_FILE = "/Users/woody/airlines.csv";
        String FLIGHTS_FILE = "/Users/woody/flights.csv";
        String DB_FILE = "/Users/woody/date.db";
        */  
        EtlImporter porter = new EtlImporter(DB_FILE);
        porter.creat_airline_table(AIRLINES_FILE);
        porter.creat_airport_table(AIRPORTS_FILE);
        porter.creat_flight_table(FLIGHTS_FILE);
        System.out.println("build database sucessfully");
    

        

        /*
         * READING DATA FROM CSV FILES
         * Source: http://opencsv.sourceforge.net/#how-to-read
         * 
         * If you want to use an Iterator style pattern, you might do something like this: 
         * 
         *    CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
         *    String [] nextLine;
         *    while ((nextLine = reader.readNext()) != null) {
         *        // nextLine[] is an array of values from the line
         *        System.out.println(nextLine[0] + nextLine[1] + "etc...");
         * }
         * 
         * Or, if you might just want to slurp the whole lot into a List, just call readAll()... 
         * 
         *    CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
         *    List myEntries = reader.readAll();
         */

        /*
         * Below are some snippets of JDBC code that may prove useful
         * 
         * For more sample JDBC code, check out 
         * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
         * 
         * ---
         * 
         *    // INITIALIZE THE CONNECTION
         *    Class.forName("org.sqlite.JDBC");
         *    Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
         *
         * ---
         *
         *    // ENABLE FOREIGN KEY CONSTRAINT CHECKING
         *    Statement stat = conn.createStatement();
         *    stat.executeUpdate("PRAGMA foreign_keys = ON;");
         *
         *    // Speed up INSERTs
         *    stat.executeUpdate("PRAGMA synchronous = OFF;");
         *    stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
         *
         * ---
         * 
         *    // You can execute DELETE statements before importing if you want to be
         *    // able to overwrite an existing database.
         *    stat.executeUpdate("DROP TABLE IF EXISTS table;");
         *
         * ---
         * 
         *     // Normally the database throws an exception when constraints are enforced
         *    // and an INSERT statement that violates a constraint is executed. This is true
         *    // even when doing a batch insert (multiple rows in one statement), causing all
         *    // rows in the statement to not be inserted into the database.
         *
         *    // As a result, if you want the efficiency gains of using batch inserts, you need to be smart:
         *    // You need to make sure your application enforces foreign key constraints before the insert ever happens.
         *     PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO table (col1, col2) VALUES (?, ?)");
         *     List<String[]> rowInfo = getTableRows();
         *  for (String[] curRow : rowInfo)
         *  {
         *      prep.setString(1, curRow[0]);
         *      prep.setInt(2, curRow[1]);
         *      prep.addBatch();
         *  }
         *  
         *  // We temporarily disable auto-commit, allowing the batch to be sent
         *  // as one single transaction. Then we re-enable it, executing the batch.
         *  conn.setAutoCommit(false);
         *  prep.executeBatch();
         *  conn.setAutoCommit(true);
         *     
         */
    }
}


