package edu.brown.cs.cs127.etl.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.text.*;
import java.io.*;

public class EtlQuery
{
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception
	{
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}

	public ResultSet query1(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(airport_code) FROM Airport;"
		);
		return stat.executeQuery();
	}

	public ResultSet query2(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"SELECT count(airline_code) FROM Airline;"
			);
			return stat.executeQuery();
		}
	
	public ResultSet query3(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"SELECT count(ID) FROM Flight;"
			);
			return stat.executeQuery();
		}
	
	public ResultSet query4(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"SELECT 'Carrier Delay' as Delay_type,sum(carrier_delay!=0) as Frequency" +
				" FROM Flight UNION ALL " +
				"SELECT 'Weather Delay' as Delay_type,sum(weather_delay!=0) as Frequency" +
				" FROM Flight UNION ALL " +
				"SELECT 'Air Traffic Delay' as Delay_type,sum(air_traffic_delay!=0) as Frequency" +
				" FROM Flight UNION ALL " +
				"SELECT 'Security Delay' as Delay_type,sum(security_concern_delay!=0) as Frequency" +
				" FROM Flight ORDER BY Frequency DESC;"
			);
			return stat.executeQuery();
	}
	
	public ResultSet query5(String[] args) throws SQLException,ParseException
	{
		PreparedStatement stat = conn.prepareStatement(
				"Select originating_airport_code," +
				"destination_airport_code,strftime('%Y-%m-%d %H:%M'," +
				"depart_date) FROM Flight WHERE " +
				"airline_code=? AND flight_num=? AND " +
				"depart_date BETWEEN ? AND ?;"
			);
		String month="";
		String day="";
		String depart_early;
		String depart_late;
		String departdate;
		if(Integer.parseInt(args[2])<10)
			 month="0"+args[2];
		else month=args[2];
		if(Integer.parseInt(args[3])<10)
			day="0"+args[3];
		else day=args[3];
		departdate=args[4]+"-"+month+"-"+day;
		depart_early=departdate+" 00:00";
		depart_late=departdate+" 23:59";
		
		stat.setString(1, args[0]);
		stat.setInt(2, Integer.parseInt(args[1]));
		stat.setString(3,depart_early);
		stat.setString(4,depart_late);	
		return stat.executeQuery();
	
	}
	
	public ResultSet query6(String[] args) throws SQLException
	{
		String depart_early;
		String depart_late;
		PreparedStatement stat = conn.prepareStatement(
				"SELECT airline_name,count(ID) FROM" +
				" Airline NATURAL JOIN FLIGHT WHERE depart_date Between ? and ? " +
				"GROUP BY airline_code " +
				"ORDER BY count(ID) DESC,airline_name ASC;"
			);
		String month="";
		String day="";
		String departdate;
		if(Integer.parseInt(args[0])<10)
			 month="0"+args[0];
		else month=args[0];
		if(Integer.parseInt(args[1])<10)
			day="0"+args[1];
		else day=args[1];
		departdate=args[2]+"-"+month+"-"+day;
		depart_early=departdate+" 00:00";
		depart_late=departdate+" 23:59";
		stat.setString(1, depart_early);
		stat.setString(2, depart_late);
		return stat.executeQuery();
	}
	
	public ResultSet query7(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"Create TEMPORARY TABLE query7_ans as " +
				"SELECT ID,originating_airport_code,destination_airport_code,depart_date,arrive_date" +
				" FROM FLIGHT WHERE (depart_date between ? and ?" +
				" OR arrive_date between ? and ?);"
			);
		int n=args.length-3;
		String month="";
		String day="";
		String chosen_date;
		if(Integer.parseInt(args[0])<10)
			 month="0"+args[0];
		else month=args[0];
		if(Integer.parseInt(args[1])<10)
			day="0"+args[1];
		else day=args[1];
		chosen_date=args[2]+"-"+month+"-"+day;
		String chosen_date_early=chosen_date+" 00:00";
		String chosen_date_late=chosen_date+" 23:59";
		stat.setString(1, chosen_date_early);
		stat.setString(2, chosen_date_late);
		stat.executeUpdate();
		PreparedStatement stat1 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query7_ans1 as " +
				"Select Airport.airport_name as airportname,count(ID) as depart_num" +
				" from query7_ans,Airport,Airport as Airport1" +
				" WHERE Airport.airport_code=originating_airport_code" +
				" AND Airport1.airport_code=destination_airport_code" +
				" AND Airport.airport_name=? AND depart_date between ? and ?;"
				);
		
		PreparedStatement stat2 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query7_ans2 as " +
				"Select Airport1.airport_name as airportname,count(ID) as arrive_num" +
				" from query7_ans,Airport,Airport as Airport1" +
				" WHERE Airport.airport_code=originating_airport_code" +
				" AND Airport1.airport_code=destination_airport_code" +
				" AND Airport1.airport_name=? AND arrive_date between ? and ?;"
				);
		stat1.setString(1, args[3]);
		stat1.setString(2, chosen_date_early);
		stat1.setString(3, chosen_date_late);
		stat2.setString(1, args[3]);
		stat2.setString(2, chosen_date_early);
		stat2.setString(3, chosen_date_late);
		stat1.executeUpdate();
		stat2.executeUpdate();

		for(int i=0;i<n-1;i++)
		{
			PreparedStatement stat4 = conn.prepareStatement(
					"INSERT INTO query7_ans1 " +
					"Select Airport.airport_name as airportname,count(ID) as depart_num" +
					" from query7_ans,Airport,Airport as Airport1" +
					" WHERE Airport.airport_code=originating_airport_code" +
					" AND Airport1.airport_code=destination_airport_code" +
					" AND Airport.airport_name=? AND depart_date between ? and ?;"
					);
			
			PreparedStatement stat5 = conn.prepareStatement(
					"INSERT INTO query7_ans2 " +
					"Select Airport1.airport_name as airportname,count(ID) as arrive_num" +
					" from query7_ans,Airport,Airport as Airport1" +
					" WHERE Airport.airport_code=originating_airport_code" +
					" AND Airport1.airport_code=destination_airport_code" +
					" AND Airport1.airport_name=? AND arrive_date between ? and ?;"
					);
			stat4.setString(1, args[4+i]);
			stat4.setString(2, chosen_date_early);
			stat4.setString(3, chosen_date_late);
			stat5.setString(1, args[4+i]);
			stat5.setString(2, chosen_date_early);
			stat5.setString(3, chosen_date_late);
			stat4.executeUpdate();
			stat5.executeUpdate();
		}
		
		PreparedStatement stat3 = conn.prepareStatement(
				"Select * from query7_ans1 NATURAL JOIN query7_ans2 ORDER BY airportname ASC;"
				);
		ResultSet rs=stat3.executeQuery();	
		return rs;
	}
	
	
	public ResultSet query8(String[] args) throws SQLException,ParseException
	{
		PreparedStatement stat = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans as " +
				"SELECT ID,cancelled,depart_time,arrive_time FROM" +
				" Flight NATURAL JOIN Airline" +
				" WHERE depart_date between ? and ?" +
				" AND flight_num=? AND airline_name=?;");
		
		SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
		Date start_date = df.parse(args[2]);//start date
		Date end_date = df.parse(args[3]);//end date
		SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
        String startdate = df2.format(start_date);  
        String enddate = df2.format(end_date);
        startdate=startdate+" 00:00";
        enddate=enddate+" 23:59";
		stat.setString(1, startdate);
		stat.setString(2, enddate);
		stat.setString(3, args[1]);
		stat.setString(4, args[0]);
		stat.executeUpdate();
		
		PreparedStatement stat1 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans1 as " +
				"SELECT count(ID) as total_num FROM query8_ans;");
		stat1.executeUpdate();
		
		PreparedStatement stat2 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans2 as " +
				"SELECT count(ID) as cancel_num  FROM" +
				" query8_ans WHERE cancelled=1;");
		stat2.executeUpdate();
		
		PreparedStatement stat3 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans3 as " +
				"SELECT count(ID) as early_depart_num FROM" +
				" query8_ans WHERE cancelled=0 AND depart_time<=0;");
		stat3.executeUpdate();
		
		PreparedStatement stat4 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans4 as " +
				"SELECT count(ID) as late_depart_num  FROM" +
				" query8_ans WHERE cancelled=0 AND depart_time>0;");
		stat4.executeUpdate();
		
		PreparedStatement stat5 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans5 as " +
				"SELECT count(ID) as early_arrive_num  FROM" +
				" query8_ans WHERE cancelled=0 AND arrive_time<=0;");
		stat5.executeUpdate();
		
		PreparedStatement stat6 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query8_ans6 as " +
				"SELECT count(ID) as late_arrive_num  FROM" +
				" query8_ans WHERE cancelled=0 AND arrive_time>0;");
		stat6.executeUpdate();
		
		PreparedStatement stat7 = conn.prepareStatement(
				"Select * FROM query8_ans1,query8_ans2,query8_ans3,query8_ans4," +
				"query8_ans5,query8_ans6;");
		return stat7.executeQuery();
	}
	
	public ResultSet query9(String[] args) throws SQLException,ParseException
	{
		PreparedStatement stat = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query9_ans AS " +
				"SELECT ID,airline_code,flight_num,originating_airport_code," +
				"destination_airport_code,depart_date,depart_time,arrive_date,arrive_time FROM Airport,Airport as Airport1,Flight" +
				" WHERE Airport.airport_code=Flight.originating_airport_code AND" +
				" Airport1.airport_code=Flight.destination_airport_code AND" +
				" Airport.city=? AND Airport.state=? AND" +
				" Airport1.city=? AND Airport1.state=? AND cancelled=0;");
		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, args[2]);
		stat.setString(4, args[3]);
		stat.executeUpdate();
		PreparedStatement stat1 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query9_ans1" +
				"(ID int PRIMARY KEY," +
				"airline_code varchar(10) NOT NULL," +
				"flight_num int," +
				"originating_airport_code varchar(10)," +
				"destination_airport_code varchar(10)," +
				"actual_depart_date varchar(20) NOT NULL," +
                "actual_arrive_date varchar(20) NOT NULL," +
                "duration int);");
		stat1.executeUpdate();
		PreparedStatement stat2 = conn.prepareStatement(
				"Select * from query9_ans;");
		ResultSet rs = stat2.executeQuery();
		
		PreparedStatement stat3 = conn.prepareStatement(
				"INSERT OR IGNORE INTO query9_ans1" +
                " VALUES(?,?,?,?,?,?,?,?);");
		
		
		while(rs.next())
		{
			int _ID=rs.getInt("ID");
			String _airline_code=rs.getString("airline_code");
			int _flight_num=rs.getInt("flight_num");
			String _originating_airport_code=rs.getString("originating_airport_code");
			String _destination_airport_code=rs.getString("destination_airport_code");
			String _depart_date=rs.getString("depart_date");
			int _depart_time=rs.getInt("depart_time");
			String _arrive_date=rs.getString("arrive_date");
			int _arrive_time=rs.getInt("arrive_time");
			
			//-----------------date processing----------------------------------
			SimpleDateFormat df1 = new SimpleDateFormat( "MM/dd/yyyy", Locale.US );
			Date input_date = df1.parse(args[4]);
			df1 = new SimpleDateFormat( "yyyy-MM-dd", Locale.US );
			String _input_date = df1.format(input_date);
			SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm", Locale.US );
			Date _depart_date_1 = df.parse(_depart_date);
			Date _arrive_date_1 = df.parse(_arrive_date);
			
			
			Calendar cal = Calendar.getInstance();
            Calendar cal1 = Calendar.getInstance();
            cal.setTime(_depart_date_1);
            cal1.setTime(_arrive_date_1);
            cal.add(Calendar.MINUTE,_depart_time);
            //add the minutes between actual and schedule depart time
            cal1.add(Calendar.MINUTE,_arrive_time);
            //add the minutes between actual and schedule arrive time
            String _actual_depart_date = df.format(cal.getTime());
            String _actual_arrive_date = df.format(cal1.getTime());	
            
            long difference=cal1.getTimeInMillis()-cal.getTimeInMillis(); 
            int _duration=(int) difference/(60*1000);
            String _actual_depart_date1=_actual_depart_date.substring(11);
            String _actual_arrive_date1=_actual_arrive_date.substring(11);
            //-----------------date processing----------------------------------
            
            if(_actual_depart_date.compareTo(_input_date+" 00:00")<0)
            	continue;
            if(_actual_arrive_date.compareTo(_input_date+" 23:59")>0)
            	continue;
            //take off and land on a single day
            stat3.setInt(1, _ID);
            stat3.setString(2, _airline_code);
            stat3.setInt(3, _flight_num);
            stat3.setString(4, _originating_airport_code);
            stat3.setString(5, _destination_airport_code);
            stat3.setString(6, _actual_depart_date1);
            stat3.setString(7, _actual_arrive_date1);
            stat3.setInt(8, _duration);
            stat3.addBatch();
		}
		conn.setAutoCommit(false);
        stat3.executeBatch();
        conn.setAutoCommit(true);
        PreparedStatement stat4 = conn.prepareStatement(
				"Select airline_code,flight_num,originating_airport_code," +
				"actual_depart_date,destination_airport_code,actual_arrive_date," +
				"duration FROM query9_ans1 ORDER BY duration,airline_code;");
        return stat4.executeQuery();
	}

	
	public ResultSet query10(String[] args) throws SQLException,ParseException
	{
		PreparedStatement stat = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query10_ans AS " +
				"SELECT ID,airline_code,flight_num,originating_airport_code," +
				"destination_airport_code,depart_date,depart_time,arrive_date,arrive_time FROM Airport,Airport as Airport1,Flight" +
				" WHERE Airport.airport_code=Flight.originating_airport_code AND" +
				" Airport1.airport_code=Flight.destination_airport_code AND" +
				" ((Airport.city=? AND Airport.state=?) OR" +
				" (Airport1.city=? AND Airport1.state=?)) AND cancelled=0;");
		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, args[2]);
		stat.setString(4, args[3]);
		stat.executeUpdate();
		
		
		PreparedStatement stat1 = conn.prepareStatement(
				"CREATE TEMPORARY TABLE query10_ans1" +
				"(ID int PRIMARY KEY," +
				"airline_code varchar(10) NOT NULL," +
				"flight_num int," +
				"originating_airport_code varchar(10)," +
				"destination_airport_code varchar(10)," +
				"actual_depart_date varchar(20) NOT NULL," +
                "actual_arrive_date varchar(20) NOT NULL," +
                "duration int);");
		stat1.executeUpdate();

		PreparedStatement stat2 = conn.prepareStatement(
				"Select * from query10_ans ;");
		ResultSet rs= stat2.executeQuery();
		//get all the depart and arrive flights possible
		
		PreparedStatement stat3 = conn.prepareStatement(
				"INSERT OR IGNORE INTO query10_ans1" +
                " VALUES(?,?,?,?,?,?,?,?);");
		
		
		while(rs.next())
		{
			int _ID=rs.getInt("ID");
			String _airline_code=rs.getString("airline_code");
			int _flight_num=rs.getInt("flight_num");
			String _originating_airport_code=rs.getString("originating_airport_code");
			String _destination_airport_code=rs.getString("destination_airport_code");
			String _depart_date=rs.getString("depart_date");
			int _depart_time=rs.getInt("depart_time");
			String _arrive_date=rs.getString("arrive_date");
			int _arrive_time=rs.getInt("arrive_time");
			
			//-----------------date processing----------------------------------
			SimpleDateFormat df1 = new SimpleDateFormat( "MM/dd/yyyy", Locale.US );
			Date input_date = df1.parse(args[4]);
			df1 = new SimpleDateFormat( "yyyy-MM-dd", Locale.US );
			String _input_date = df1.format(input_date);
			SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd HH:mm", Locale.US );
			Date _depart_date_1 = df.parse(_depart_date);
			Date _arrive_date_1 = df.parse(_arrive_date);
			
			
			Calendar cal = Calendar.getInstance();
            Calendar cal1 = Calendar.getInstance();
            cal.setTime(_depart_date_1);
            cal1.setTime(_arrive_date_1);
            cal.add(Calendar.MINUTE,_depart_time);
            //add the minutes between actual and schedule depart time
            cal1.add(Calendar.MINUTE,_arrive_time);
            //add the minutes between actual and schedule arrive time
            String _actual_depart_date = df.format(cal.getTime());
            String _actual_arrive_date = df.format(cal1.getTime());	
            
            long difference=cal1.getTimeInMillis()-cal.getTimeInMillis(); 
            int _duration=(int) difference/(60*1000);
            String _actual_depart_date1=_actual_depart_date.substring(11);
            String _actual_arrive_date1=_actual_arrive_date.substring(11);
            //-----------------date processing----------------------------------
            
            if(_actual_depart_date.compareTo(_input_date+" 00:00")<0)
            	continue;
            if(_actual_arrive_date.compareTo(_input_date+" 23:59")>0)
            	continue;
            //take off and land on a single day for every flight still stands
            stat3.setInt(1, _ID);
            stat3.setString(2, _airline_code);
            stat3.setInt(3, _flight_num);
            stat3.setString(4, _originating_airport_code);
            stat3.setString(5, _destination_airport_code);
            stat3.setString(6, _actual_depart_date1);
            stat3.setString(7, _actual_arrive_date1);
            stat3.setInt(8, _duration);
            stat3.addBatch();
		}
		conn.setAutoCommit(false);
        stat3.executeBatch();
        conn.setAutoCommit(true);
        //now we get all the flights possible with the correct time presentation
        
       /* PreparedStatement stat4 = conn.prepareStatement(
				"Select airline_code,flight_num,originating_airport_code," +
				"actual_depart_date,destination_airport_code,actual_arrive_date," +
				"duration FROM query10_ans1 ORDER BY duration,airline_code;");
        return stat4.executeQuery();
        */
         PreparedStatement stat4 = conn.prepareStatement(
        		"SELECT DEPART_TABLE.airline_code as airlinecode," +
        		"DEPART_TABLE.flight_num as flightnum," +
        		"DEPART_TABLE.originating_airport_code as departairportcode," +
        		"DEPART_TABLE.actual_depart_date as depttime," +
         		"DEPART_TABLE.destination_airport_code as arriveairportcode," +
        		"DEPART_TABLE.actual_arrive_date as arrivetime," +
        		"ARRIVE_TABLE.airline_code as airlinecode1," +
        		"ARRIVE_TABLE.flight_num as flightnum1," +
        		"ARRIVE_TABLE.originating_airport_code as departairportcode1," +
        		"ARRIVE_TABLE.actual_depart_date as depttime1," +
         		"ARRIVE_TABLE.destination_airport_code as arrive_airportcode1," +
        		"ARRIVE_TABLE.actual_arrive_date as arrivetime1," +
        		"(strftime('%s',ARRIVE_TABLE.actual_arrive_date) - " +
        		"strftime('%s',DEPART_TABLE.actual_depart_date))/60 as duration" +
        		" FROM" +
        		" query10_ans1 as DEPART_TABLE,query10_ans1 AS ARRIVE_TABLE," +
        		"Airport as DEPART_TABLE_Airport,Airport as ARRIVE_TABLE_Airport"+
        		" WHERE DEPART_TABLE.originating_airport_code=DEPART_TABLE_Airport.airport_code AND" +
        		" ARRIVE_TABLE.destination_airport_code=ARRIVE_TABLE_Airport.airport_code AND" +
        		" DEPART_TABLE_Airport.city=? AND DEPART_TABLE_Airport.state=? AND" +
        		" ARRIVE_TABLE_Airport.city=? AND ARRIVE_TABLE_Airport.state=? AND" +
        		" DEPART_TABLE.destination_airport_code=ARRIVE_TABLE.originating_airport_code AND" +
        		" DEPART_TABLE.actual_arrive_date<ARRIVE_TABLE.actual_depart_date" +
        		" ORDER BY duration,DEPART_TABLE.airline_code,ARRIVE_TABLE.airline_code;"
        		);
        stat4.setString(1, args[0]);
        stat4.setString(2, args[1]);
        stat4.setString(3, args[2]);
        stat4.setString(4, args[3]);
        return stat4.executeQuery();
	}
	
	public ResultSet query11(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"SELECT airport_name and sum(flight_num) FROM" +
				" Airline NATURE JOIN FLIGHT WHERE depart_date=? " +
				"GROUP BY airline_code;"
			);
		String month="";
		String day="";
		String departdate;
		if(Integer.parseInt(args[0])<10)
			 month="0"+args[0];
		else month=args[0];
		if(Integer.parseInt(args[1])<10)
			day="0"+args[1];
		else day=args[1];
		departdate=args[3]+"-"+month+"-"+day;
		stat.setString(1, departdate);
		return stat.executeQuery();
	}
}
