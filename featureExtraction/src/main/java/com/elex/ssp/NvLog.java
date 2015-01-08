package com.elex.ssp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class NvLog {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		File file = new File(args[0]);

		process(file,args[1]);
	}

	private static void process(File file,String dist) throws IOException {

		File[] logs = file.listFiles();

		BufferedReader in;

		BufferedWriter out = new BufferedWriter(new FileWriter(dist));

		String[] kv;
		String reqid="null",nation="null",uid="null",type="null",from="null";
		for (File log : logs) {
			in = new BufferedReader(new FileReader(log));
			String line = in.readLine();
			while (line != null ) {
					kv = line.trim().split("\t");
					if(kv.length==6){
						try{							

							type=kv[4].contains("type=")?kv[4].substring(kv[4].indexOf("type=")+5,kv[4].indexOf("&", kv[4].indexOf("type=")+5)==-1?kv[4].length():kv[4].indexOf("&", kv[4].indexOf("type=")+5)):type;
							from=kv[4].contains("from=")?kv[4].substring(kv[4].indexOf("from=")+5,kv[4].indexOf("&", kv[4].indexOf("from=")+5)==-1?kv[4].length():kv[4].indexOf("&", kv[4].indexOf("from=")+5)):from;
							uid=kv[5].substring(kv[5].indexOf("User_id=")+8,kv[5].indexOf("&", kv[5].indexOf("User_id=")+8)==-1?kv[5].length():kv[5].indexOf("&", kv[5].indexOf("User_id=")+8));
							reqid=kv[5].substring(kv[5].indexOf("reqID=")+6,kv[5].indexOf("&", kv[5].indexOf("reqID=")+6)==-1?kv[5].length():kv[5].indexOf("&", kv[5].indexOf("reqID=")+6));
							nation=kv[5].substring(kv[5].indexOf("User_nation=")+12,kv[5].indexOf("&", kv[5].indexOf("User_nation=")+12)==-1?kv[5].length():kv[5].indexOf("&", kv[5].indexOf("User_nation=")+12));
							out.write(reqid+" "+uid+" "+nation+" "+type+" "+from+"\r\n");
						}catch (StringIndexOutOfBoundsException e){
							System.out.println(line);
						}
					}
					
					line = in.readLine();
			}				
			in.close();
		}
		
		out.close();

	}
}
