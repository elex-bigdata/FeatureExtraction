package com.elex.ssp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.feature.tfidf.IDF;
import com.elex.ssp.feature.tfidf.TF;

public class Scheduler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static int tfidf() throws Exception{
		int a = ToolRunner.run(new Configuration(), new TF(), null);
		int b = ToolRunner.run(new Configuration(), new IDF(), null);
		return Math.max(a, b);
	}

}
