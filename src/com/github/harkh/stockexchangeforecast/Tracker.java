package com.github.harkh.stockexchangeforecast;

import java.lang.Thread;
import java.math.BigDecimal;
import yahoofinance.*;

public class Tracker extends Thread
{
	public Tracker(String symbolInput, int frequencyInput)
	{
		symbol = symbolInput;
		price = new BigDecimal(-1);
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
				// Download the price at the moment
				Stock stock = YahooFinance.get(symbol);
				price = stock.getQuote().getPrice();
				
				// Parse the data
				//parser.parse(price);
				
				// Wait to the next data update
				Thread.sleep(frequency);
			}
			catch (Exception e)
			{
				Log.write("Error --> Tracker.run");
			}
		}
	}
	
	public void stopRunning()
	{
	    status = false;
	    Log.write(symbol + " tracker has been stopped !");
	}
	
	String symbol;
	BigDecimal price;
	int frequency;
	boolean status;
}
