package com.github.harkh.stockexchangeforecast;

import java.sql.*;

public class Main 
{
	public static void main(String[] args)
	{
	    Log.write("iTrade started !");
	    
	    try
	    {
	        Connection conn = Manager.connectionToDatabase();
	        
	        if (conn != null)
	        {
	            Statement state0 = conn.createStatement();
	            ResultSet result = state0.executeQuery("SELECT * FROM \"MarketAction\"");
	            
	            int rowCount = Manager.getTableSize("MarketAction", conn);
	            
	            Tracker trackerPool[] = new Tracker[rowCount];
	            
	            int i = 0;
	            while (result.next())
	            {
	                Manager.clean(result.getString(3));
	                Manager.complete(result.getString(3));
	               
	                trackerPool[i] = new Tracker(result.getString(3), 5);
	                //trackerPool[i].start();
	                
	                i++;
	            }
	            
	            // Updater will check data every 5 minutes
	            Updater updater = new Updater(5);
	            updater.start();
	            
	            Log.write("Updater launched !");
	        }
	    }
	    catch (Exception e)
	    {
	        Log.write("Error --> Main : " + e.getMessage());
	    }
	}
}
