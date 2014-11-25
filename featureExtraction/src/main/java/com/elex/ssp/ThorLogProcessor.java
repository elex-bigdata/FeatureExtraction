package com.elex.ssp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

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

    
	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		process(new File(args[0]),args);

	}

	public static void process(File file,String[] args) throws IOException{
		BufferedReader in = new BufferedReader(new FileReader(file));
		File predict_file = new File(args[1]);
		BufferedWriter predict = new BufferedWriter(new FileWriter(predict_file));	
		File fv_file = new File(args[2]);
		BufferedWriter fv = new BufferedWriter(new FileWriter(fv_file));	
		String line = in.readLine();
		String[] logs = new String[3];
		String[] kv,fv_line,fv_array;
		String reqid=null,adid=null,predict_v = null,ft=null;
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
			
			logs[0]=line.trim();			
			if(logs[0]!=null){
				if(logs[0].contains("calculate score spend")){
					reqid = logs[0].substring(24, logs[0].indexOf("calculate")).trim();	
				}
			}
			
			logs[1]=in.readLine();
			if(logs[1]!=null){
				if(logs[1].contains("Top score")){
					kv=logs[1].substring(logs[1].indexOf("(")+1,logs[1].indexOf(")")).split(",");
					if(kv.length==2){
						adid=kv[0];
						predict_v=kv[1];
					}				
				}
			}
			
						
			predict.write(reqid+","+adid+","+predict_v+"\r\n");
			
			logs[2]=in.readLine();
			if(logs[2]!=null){
				if(logs[2].contains("MSG")){
					fv_line=logs[2].substring(24,logs[2].length()).split("\t");
					if(fv_line.length==9){
						fv_array=fv_line[8].split("\\.");
						for(int i=0;i<fv_array.length;i++){
							if(fv_array[i].contains("_")){
								ft=ftMap.get(fv_array[i].substring(0,fv_array[i].indexOf("_")));
								for(String v:fv_array[i].substring(fv_array[i].indexOf("_")+1, fv_array[i].length()).split("_")){
									fv.write(reqid+","+ft+","+v+"\r\n");
								}
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
