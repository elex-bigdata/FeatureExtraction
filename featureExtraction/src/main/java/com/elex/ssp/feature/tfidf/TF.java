package com.elex.ssp.feature.tfidf;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.WordSeder;
import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HdfsUtil;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class TF extends Configured implements Tool{

	 public static Counter ct = null;
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		ToolRunner.run(new Configuration(), new TF(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "tf");
		job.setJarByClass(TF.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		String uri = PropertiesUtils.getRootDir() + Constants.USERDOCS;
		prepareInput(uri);
		Path in = new Path(uri);							
		FileInputFormat.addInputPath(job, in);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "tf", TextOutputFormat.class, Text.class, Text.class);
		
		Path output = new Path(PropertiesUtils.getRootDir() + Constants.TF);
		HdfsUtil.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		
		String userCount = PropertiesUtils.getRootDir() + Constants.USERCOUNT;
		ct = job.getCounters().findCounter("HAS_QUERY","USER_COUNT");
		HdfsUtil.writeInt(new Long(ct.getValue()).intValue(), new Path(userCount), conf);
		
		return result;
	}
	
	public static void prepareInput(String uri) throws SQLException{
		String day = Constants.getStartDay();
		String sql = "select uid,CONCAT_WS(' ',collect_set(query)) from query_en where day >'"+day+"' group by uid";
		System.out.println("=================TF-prepareInput-sql===================");
		System.out.println(sql);
		System.out.println("=================TF-prepareInput-sql===================");
		HiveOperator.exportHdfs(uri, sql);		
	}
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		
		private Map<String,Integer> user = new HashMap<String,Integer>();
		private Iterator<Entry<String, Integer>> ite;
		private Entry<String, Integer> entry;
		private DecimalFormat df = Constants.df;
		private int wc = 0;
		private MultipleOutputs<Text, Text> tf;  
		private String[] kv;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
		    tf = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			kv = value.toString().split("\\x01");
			if(kv.length==2){
				wc = 0;
				user.clear();
				for(String word :WordSeder.sed(kv[1])){
					user.put(word, user.get(word)==null?1:user.get(word)+1);
					wc++;
				}
				
				ite = user.entrySet().iterator();
				while(ite.hasNext()){
					entry = ite.next();
					tf.write(new Text(kv[0]+","+entry.getKey()+","+entry.getValue()+","+df.format(new Double(entry.getValue())/new Double(wc))), null, "tf");
					context.write(new Text(entry.getKey()), new Text("1"));
				}
				context.getCounter("HAS_QUERY","USER_COUNT").increment(1);
			}
			
						
			
		}
		
		/**
		 * 释放资源
		 */
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			tf.close();
		}
		 				
	}
	
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		int docs=0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			docs=0;
			
			for(Text v:values){
				docs++;
			}
			
			context.write(null, new Text(key.toString()+","+Integer.toString(docs)));
		}
				
	}
				

}
