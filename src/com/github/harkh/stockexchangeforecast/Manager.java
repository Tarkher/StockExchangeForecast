package com.github.harkh.stockexchangeforecast;

import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

public class Manager 
{	
    public static Connection connectionToDatabase()
    {
        String urlDatabase = "jdbc:postgresql://localhost:12345/TheSpiceMelange";
        String login = "postgres";
        String password = "babar1";
        
        Connection conn = null;
        
        try
        {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(urlDatabase, login, password);
        }
        catch (Exception e)
        {
            Log.write("Error --> Manager.connectionToDatabase : " + e.getMessage());
        }
        
        return conn;
    }
    
	public static void addToDatabase(String symbol, String name, int openHour, int closeHour, String timeZone)
	{	
		try
		{
			Connection conn = connectionToDatabase();
			
			if (conn != null)
			{
				Statement state0 = conn.createStatement();
				state0.executeUpdate("INSERT INTO \"MarketAction\" (name, symbol, \"openHour\", \"closeHour\", \"timeZone\", \"lastUpdate\")"
									+ "VALUES ('" + name + "',"
									+ "'" + symbol + "',"
									+ Integer.toString(openHour) + ","
									+ Integer.toString(closeHour) + ","
									+ "'" + timeZone + "',"
									+ "'0001-01-01 00:00:00'"
									+ ")");
				state0.close();
				
				Statement state1 = conn.createStatement();
				state1.executeUpdate("CREATE TABLE \"" + symbol + "\""
									+ "("
									+  "\"ID\" serial primary key not null,"
									+  "high double precision,"
									+  "low double precision,"
									+  "open double precision,"
									+  "close double precision,"
									+  "date timestamp without time zone"
									+ ")");
				state1.close();
				
		        Log.write("[ADD]" + symbol + " has been added to the list !");
			}
			
			conn.close();
		}
		catch (Exception e)
		{
			Log.write("[ERROR]Manager.addToDatabase : " + e.getMessage());
		}
	}
	
	public static void delete(String symbol)
    {
        try
        {
            Connection conn = connectionToDatabase();
            
            if (conn != null)
            {
	            Statement state0 = conn.createStatement();
	            state0.executeUpdate("DELETE FROM \"MarketAction\" WHERE symbol = '" + symbol + "'");
	            state0.close();
	            
	            Statement state1 = conn.createStatement();
	            state1.executeUpdate("DROP TABLE \"" + symbol + "\"");
	            state1.close();
	            
	            Log.write("[DELETE]" + symbol + " has been deleted from the database !");
            }
            
            conn.close();
            
        }
        catch (Exception e)
        {
            Log.write("[ERROR]Manager.delete : " + e.getMessage());
        }
    }
	
	public static void complete(String symbol)
    {
        try
        {
            Connection conn = connectionToDatabase();
            
            if (conn != null)
            {
	            int rowCount = getTableSize(symbol, conn);

	            Statement state = conn.createStatement();
	            ResultSet r = state.executeQuery("SELECT * FROM \"MarketAction\" WHERE symbol = '" + symbol + "'");
	            r.next();
	            
	            Calendar firstUpdate = Calendar.getInstance(TimeZone.getTimeZone(r.getString(6)));
	            firstUpdate.setTimeInMillis(r.getTimestamp(8).getTime());
	            
	            Calendar lastUpdate = Calendar.getInstance(TimeZone.getTimeZone(r.getString(6)));
	            lastUpdate.setTimeInMillis(r.getTimestamp(7).getTime());
	            
	            Stock stock = YahooFinance.get(symbol, firstUpdate, lastUpdate, Interval.DAILY);
	            List<HistoricalQuote> HistQuotes = stock.getHistory();
	            
	            if (rowCount < HistQuotes.size())
	            {
		            Statement state0 = conn.createStatement();
		            ResultSet result = state0.executeQuery("SELECT * FROM \"" + symbol + "\" ORDER BY \"date\" ASC");
		            
		            Calendar dataEntries[] = new Calendar[rowCount];
		            
	                int i = 0;
	                while (result.next())
	                {
	                    dataEntries[i] = Calendar.getInstance();
	                    dataEntries[i].setTimeInMillis(result.getTimestamp(6).getTime());
	                    
	                    i++;
	                }

		            int k = 0;
		            int j = HistQuotes.size() - 1;
		            
		            while (k < rowCount && j >= 0)
		            {
		                if (!HistQuotes.get(j).getDate().equals(dataEntries[k]))
		                {             
		                    Timestamp timestamp0 = new Timestamp(HistQuotes.get(j).getDate().getTimeInMillis());
		                    
		                    Statement state1 = conn.createStatement();
		                    state1.executeUpdate("INSERT INTO \"" + symbol + "\" (high, low, open, close, date) VALUES("
		                            + HistQuotes.get(j).getHigh().toString() + ", "
		                            + HistQuotes.get(j).getLow().toString() + ", "
		                            + HistQuotes.get(j).getOpen().toString() + ", "
		                            + HistQuotes.get(j).getClose().toString() + ", "
		                            + "'" + timestamp0.toString().substring(0, 19) + "')");
		                    state1.close();
		                    
		                    Log.write("[COMPLETE]" + symbol + " : " + timestamp0.toString().substring(0, 19));
		                }
		                else
		                {
		                    k++;
		                }
		                
		                j--;
		            }  
	            }
            }
            
            conn.close();
        }
        catch (Exception e)
        {
            Log.write("[ERROR]Manager.complete" + e.getMessage());
            e.printStackTrace();
        }
    }
	
	public static void extendDataRange(String symbol, Calendar from)
	{
		try
        {
            Connection conn = connectionToDatabase();
            
            if (conn != null)
            {
	            Statement state = conn.createStatement();
	            ResultSet r = state.executeQuery("SELECT * FROM \"MarketAction\" WHERE symbol = '" + symbol + "'");
	            
	            Calendar firstUpdate = Calendar.getInstance(TimeZone.getTimeZone(r.getString(6)));
	            firstUpdate.setTimeInMillis(r.getTimestamp(8).getTime());
	            firstUpdate.add(Calendar.DATE, -1);
	            
	            if (from.before(firstUpdate))
	            {
		            Stock stock = YahooFinance.get(symbol, from, firstUpdate, Interval.DAILY);
		            List<HistoricalQuote> HistQuotes = stock.getHistory();
		            
		            for (int i = 0; i < HistQuotes.size(); i++)
		            {
		            	Timestamp timestamp0 = new Timestamp(HistQuotes.get(i).getDate().getTimeInMillis());
	                    
	                    Statement state1 = conn.createStatement();
	                    state1.executeUpdate("INSERT INTO \"" + symbol + "\" (high, low, open, close, date) VALUES("
	                            + HistQuotes.get(i).getHigh().toString() + ", "
	                            + HistQuotes.get(i).getLow().toString() + ", "
	                            + HistQuotes.get(i).getOpen().toString() + ", "
	                            + HistQuotes.get(i).getClose().toString() + ", "
	                            + "'" + timestamp0.toString().substring(0, 19) + "')");
	                    state1.close();
	                    
	                    Log.write("[EXTEND]" + symbol + " : " + timestamp0.toString().substring(0, 19));
		            }	            
	            }
	            
	            Timestamp timestampTemp = new Timestamp(from.getTimeInMillis());
	            Statement state0 = conn.createStatement();
	            state0.executeUpdate("UPDATE \"MarketAction\" SET \"firstUpdate\" = '" + timestampTemp.toString().substring(0, 19) + "' WHERE symbol = '" + symbol + "'");
	            state0.close();
	            
	            state.close();
            }
            
            conn.close();
        }
		catch (Exception e)
		{
			Log.write("Error --> Manager.extendDataRange : " + e.getMessage());
		}
	}
	
	public static void clean(String symbol)
	{
	    try
	    {
	        Connection conn = connectionToDatabase();
            
	        if (conn != null)
	        {
	            Statement state0 = conn.createStatement();
	            ResultSet result = state0.executeQuery("SELECT * FROM \"" + symbol + "\" ORDER BY \"date\" ASC");
	            
	            result.next();
	            Calendar dateTemp0 = Calendar.getInstance();
	            dateTemp0.setTimeInMillis(result.getTimestamp(6).getTime());
	            
	            while (result.next())
	            {
	                Calendar dateTemp1 = Calendar.getInstance();
	                dateTemp1.setTimeInMillis(result.getTimestamp(6).getTime());
	                
	                if (dateTemp0.equals(dateTemp1))
	                {
	                    Statement state1 = conn.createStatement();
	                    state1.executeUpdate("DELETE FROM \"" + symbol + "\" WHERE \"ID\" = " + result.getInt(1));
	                    state1.close();
	                    
	                    Log.write("[CLEAN]" + symbol + " : " + result.getTimestamp(6).toString().substring(0, 19));
	                }
	                
	                dateTemp0 = dateTemp1;
	            }
	        }
	        
	        conn.close();
	    }
	    catch (Exception e)
	    {
	        Log.write("[ERROR]Manager.clean : " + e.getMessage());
	    }
	}
	
	public static int getTableSize(String tableName, Connection conn)
	{
		int rowCount = 0;
		
		try
		{
			conn = Manager.connectionToDatabase();
			
			if (conn != null)
			{
				Statement state = conn.createStatement();
		        ResultSet result = state.executeQuery("SELECT COUNT(*) AS rowCount FROM \"" + tableName + "\"");
		        
		        result.next();
		        rowCount = result.getInt("rowCount");
		        result.close();
			}
		}
		catch (Exception e)
		{
			Log.write("Error --> Manager.getTableSize : " + e.getMessage());
		}
		
		return rowCount;
	}
}
