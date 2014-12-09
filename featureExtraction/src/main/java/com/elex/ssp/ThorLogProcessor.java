package com.elex.ssp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;

public class ThorLogProcessor {

	public static final String QUERY = "q";
    public static final String QUERY_LENGTH = "ql";
    public static final String QUERY_WORD_COUNT = "wc";
    public static final String KEYWORD = "kw";
    public static final String TIME = "t";
    public static final String Area = "ip";
    public static final String BROWSER = "bw";
    public static final String USER = "u";
    public static final String PROJECT = "p";
    public static final Calendar cal = Calendar.getInstance();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    
	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		process(new File(args[0]),args);

	}

	public static void process(File file,String[] args) throws IOException, ParseException{
		BufferedReader in = new BufferedReader(new FileReader(file));
		File predict_file = new File(args[1]);
		BufferedWriter predict = new BufferedWriter(new FileWriter(predict_file));	
		File fv_file = new File(args[2]);
		BufferedWriter fv = new BufferedWriter(new FileWriter(fv_file));	
		String line = in.readLine();
		String[] fv_line,fv_array;
		String reqid=null,ft=null,day=null;
		Map<String,String> ftMap = new HashMap<String,String>();
		ftMap.put("q", "query");
		ftMap.put("ql", "query_length");
		ftMap.put("wc", "query_word_count");
		ftMap.put("kw", "keyword");
		ftMap.put("t", "time");
		ftMap.put("ip", "area");
		ftMap.put("bw", "browser");
		ftMap.put("u", "user");
		ftMap.put("p", "project");
		
		while(line != null){
			
			line=line.trim();									
			
			if(line!=null){
				if(line.contains("MSG")){
					fv_line=line.substring(24,line.length()).split("\t");
					day = line.trim().substring(0,19);
					cal.setTime(sdf.parse(day));
					day = DateFormatUtils.formatUTC(cal.getTimeInMillis(), "yyyy-MM-dd");
					if(fv_line.length==10){
						reqid=fv_line[1];
						predict.write(day+","+fv_line[1]+","+fv_line[5]+","+fv_line[6]+"\r\n");
						fv_array=fv_line[9].split(";");						
						for(int i=0;i<fv_array.length;i++){
							ft=ftMap.get(fv_array[i].substring(0,fv_array[i].indexOf(",")));
							
							for(String v:fv_array[i].substring(fv_array[i].indexOf(",")+1, fv_array[i].length()).split(",")){
								fv.write(day+","+reqid+","+ft+","+v+"\r\n");
							}																					
						}
						
					}
				}
			}
					
			line=in.readLine();
		}
		
		fv.close();
		predict.close();
		in.close();
			 		
	}

}
