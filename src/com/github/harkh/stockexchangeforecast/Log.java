package com.github.harkh.stockexchangeforecast;

import java.util.Calendar;
import java.io.FileOutputStream;

public class Log
{
    public static void write(String msg)
    {
        try
        {
            Calendar timeATM = Calendar.getInstance();
            
            String year =String.format("%02d",timeATM.get(Calendar.YEAR));
            String month =String.format("%02d",timeATM.get(Calendar.MONTH));
            String day =String.format("%02d",timeATM.get(Calendar.DATE));
            
            String fileName = year + "-" + month + "-" + day + ".log";
            
            FileOutputStream output = new FileOutputStream(fileName, true);
            
            String hour =String.format("%02d",timeATM.get(Calendar.HOUR_OF_DAY));
            String minute =String.format("%02d",timeATM.get(Calendar.MINUTE));
            String second =String.format("%02d",timeATM.get(Calendar.SECOND));
            
            String header = "[" + hour + ":" + minute + ":" + second + "] ";
            
            String msgToWrite = "\n" + header + msg;
            
            output.write(msgToWrite.getBytes());
            output.close();
        }
        catch (Exception e)
        {
            System.out.println("Error --> Log.write");
            e.printStackTrace();
        }
    }
}
