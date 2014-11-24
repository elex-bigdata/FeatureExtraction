package com.elex.ssp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Random;

import com.elex.ssp.common.Constants;

public class FeatureSelect {

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		process(new File("D:\\数据分析\\odin\\br\\brlog.txt"),0.1);

	}

	public static void process(File file,double n) throws IOException, ParseException{
		BufferedReader in = new BufferedReader(new FileReader(file));
		Random random = new Random();
		File destFile = new File("D:\\数据分析\\odin\\br\\attr.csv");
		BufferedWriter out = new BufferedWriter(new FileWriter(destFile));	
		String line = in.readLine();
		String[] attr,timeDim;
		String reqid,uid,pid,ip_area,nation,ua,os,w,h,pv,adid,impr,time,click,query,sv,dt,day,hour,daypart,work;
		out.write("uid,pid,ip_area,nation,ua,os,adid,hour,daypart,work,query,class"+"\r\n");
		while(line != null){			
			attr = line.trim().split(",");
			if(random.nextDouble()<n){
				if(attr.length==18){
					uid=Constants.getIntFromStr(attr[1]);
					pid=attr[2];
					ip_area=Constants.getArea(attr[3]);
					nation=attr[4];
					ua=attr[5];
					os=attr[6];
					adid=attr[10];
					String[] args = {attr[12],nation};
					timeDim = TimeUtils.getTimeDimension(args);
					hour = timeDim[0];
					daypart=timeDim[1];
					work=timeDim[2];
					query=Constants.getIntFromStr(attr[14]);
					click=attr[13].equals("\\N")?"0":"1";
					out.write(uid+","+pid+","+ip_area+","+nation+","+ua+","+os+","+adid+","+hour+","+daypart+","+work+","+query+","+click+"\r\n");
				}
				
			}
			line = in.readLine();
		}
		
		out.close();
		in.close();
			 		
	}

}
