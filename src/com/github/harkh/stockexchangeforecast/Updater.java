package com.github.harkh.stockexchangeforecast;

import java.lang.Thread;
import java.sql.*;
import yahoofinance.*;
import yahoofinance.histquotes.*;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.List;
import java.io.FileNotFoundException;

public class Updater extends Thread
{
	public Updater(int frequencyInput)
	{
		frequency = frequencyInput * (1000 * 60);
		status = false;
	}
	
	public void run()
	{		
		status = true;
		
		while (status)
		{
			try
			{
				// Looking for a database with outdated data
			    Connection conn = Manager.connectionToDatabase();
			    
				Statement state0 = conn.createStatement();
				ResultSet result = state0.executeQuery("SELECT * FROM \"MarketAction\"");
				
				while (result.next())
				{
				    // Set the date of the next close after the last update
					int hourClose = result.getInt(5) / 100;
					int minuteClose = result.getInt(5) % 100;
       
					Calendar lastUpdate = Calendar.getInstance(TimeZone.getTimeZone(result.getString(6)));
					lastUpdate.setTime(result.getTimestamp(7));
					
					Calendar nextClose = lastUpdate;
					if ((nextClose.get(Calendar.HOUR_OF_DAY) > hourClose) ||
					     (nextClose.get(Calendar.HOUR_OF_DAY) == hourClose && nextClose.get(Calendar.MINUTE) >= minuteClose))
					{
					    nextClose.add(Calendar.DATE, 1);
					}
					
					// Saturday and Sunday are close day
					while(nextClose.get(Calendar.DAY_OF_WEEK) == 1 || nextClose.get(Calendar.DAY_OF_WEEK) == 7)
					{
					    nextClose.add(Calendar.DATE, 1);
					}
					
					nextClose.set(Calendar.HOUR_OF_DAY, hourClose);
					nextClose.set(Calendar.MINUTE, minuteClose);
					
					Calendar timeATM = Calendar.getInstance(TimeZone.getTimeZone(result.getString(6)));
					
					if (timeATM.after(nextClose))
					{ 
						// Request to Yahoo Finance to update the database
						List<HistoricalQuote> HistQuotes;
						
						try
						{
						    Stock stock = YahooFinance.get(result.getString(3), lastUpdate, timeATM, Interval.DAILY);
						    HistQuotes = stock.getHistory();
						}
						catch (FileNotFoundException e)   // Yahoo doesn't already get the fresh data
						{
						    continue;
						}
						
						for (int i = 0; i < HistQuotes.size(); i++)
						{
							Timestamp timestamp0 = new Timestamp(HistQuotes.get(i).getDate().getTimeInMillis());
	
							Statement state1 = conn.createStatement();
							state1.executeUpdate("INSERT INTO \"" + result.getString(3) + "\" (high, low, open, close, date) VALUES("
									+ HistQuotes.get(i).getHigh().toString() + ", "
									+ HistQuotes.get(i).getLow().toString() + ", "
									+ HistQuotes.get(i).getOpen().toString() + ", "
									+ HistQuotes.get(i).getClose().toString() + ", "
									+ "'" + timestamp0.toString().substring(0, 19) + "')");
							state1.close();
							
							Log.write("[UPDATE]" + result.getString(3) + " : " + timestamp0.toString().substring(0, 19));
						}
						
						Timestamp timestamp1 = new Timestamp(Calendar.getInstance().getTimeInMillis());
						
						Statement state2 = conn.createStatement();
						state2.executeUpdate("UPDATE \"MarketAction\" SET \"lastUpdate\" = '" + timestamp1.toString().substring(0, 19) + "' WHERE symbol = '" + result.getString(3) + "'");
						state2.close();
					}
				}
	
				state0.close();
				
				conn.close();
				
				// Wait to the next data update
				Thread.sleep(frequency);
			}
			catch (Exception e)
			{
				Log.write("Error --> Updater.run : " + e.getMessage());
				stopRunning();
			}
		}
	}
	
	public void stopRunning()
	{
	    status = false;
	    Log.write("[UPDATER]Updater has been stopped !");
	}

	int frequency;
	boolean status;
}

