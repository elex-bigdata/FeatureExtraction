package com.elex.ssp.common;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Constants {

	public static final String USERDOCS="/tfidf/userdocs";
	public static final String TF="/tfidf/tf";
	public static final String IDF="/tfidf/idf";
	public static final String USERCOUNT="/tfidf/uc.norm";
	public static final String TFIDFTABLE="tfidf";
	public static final DecimalFormat df = new DecimalFormat("#.####");
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	public static String getStartDay(){
		Calendar ca = Calendar.getInstance();
		
		Date now = new Date();
		int mergeDays = PropertiesUtils.getMergeDays()+1;		
		ca.setTime(now);
		ca.add(Calendar.DAY_OF_MONTH, -mergeDays);
		String day = sdf.format(ca.getTime());	
		return day;
	}
	
	public static String getToday(){
		return sdf.format(new Date());
	}
}
