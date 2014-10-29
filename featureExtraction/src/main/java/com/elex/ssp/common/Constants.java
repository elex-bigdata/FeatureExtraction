package com.elex.ssp.common;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Vector;

public class Constants {

	public static final String USERDOCS="/tfidf/userdocs";
	public static final String TF="/tfidf/tf";
	public static final String IDF="/tfidf/idf";
	public static final String USERCOUNT="/tfidf/uc.norm";
	public static final String TFIDFTABLE="tfidf";
	public static final DecimalFormat df = new DecimalFormat("#.####");
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	public static final String UDFJAR = "/home/hadoop/wuzhongju/ssp/feUDF-1.0.jar";

	public static String getStartDay(){
		Calendar ca = Calendar.getInstance();
		
		Date now = new Date();
		int mergeDays = PropertiesUtils.getMergeDays()+1;		
		ca.setTime(now);
		ca.add(Calendar.DATE, -mergeDays);
		String day = sdf.format(ca.getTime());	
		return day;
	}
	
	public static String getToday(){
		return sdf.format(new Date());
	}
	
	public static String getYestoday(){
        Calendar ca = Calendar.getInstance();
		
		Date now = new Date();
		ca.setTime(now);
		ca.add(Calendar.DATE, -1);
		return sdf.format(ca.getTime());
	}
	
	public static String[]  getBetweenDate(String d1,String d2) throws ParseException
    {
        Vector<String> v=new Vector<String>();
        GregorianCalendar gc1=new GregorianCalendar(),gc2=new GregorianCalendar();
        gc1.setTime(sdf.parse(d1));
        gc2.setTime(sdf.parse(d2));
        do{
            GregorianCalendar gc3=(GregorianCalendar)gc1.clone();
            v.add(sdf.format(gc3.getTime()));
            gc1.add(Calendar.DAY_OF_MONTH, 1);             
         }while(!gc1.after(gc2));
        return v.toArray(new String[v.size()]);
    } 
	
	public static void main(String[] args) throws ParseException{
		for(String a:getBetweenDate("20140918","20141028")){
			System.out.println(a);
		}
		
		System.out.println(getYestoday());
	}
}
