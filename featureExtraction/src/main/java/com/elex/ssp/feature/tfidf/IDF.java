package com.elex.ssp.feature.tfidf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HdfsUtil;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class IDF extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Configuration(), new IDF(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "idf");
		job.setJarByClass(IDF.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);

		Path in = new Path(PropertiesUtils.getRootDir() + Constants.TF);
		FileStatus[] files = fs.listStatus(in, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				String name = path.getName();
				return name.startsWith("tf") && !name.endsWith(".crc");
			}
		});

		for (FileStatus file : files) {
			if (file.isFile()) {
				FileInputFormat.addInputPath(job, file.getPath());
			}

		}

		job.setOutputFormatClass(TextOutputFormat.class);
		Path output = new Path(PropertiesUtils.getRootDir() + Constants.IDF);
		HdfsUtil.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);

		int result = job.waitForCompletion(true) ? 0 : 1;
		if(result==0){
			loadResultToHive(output);
		}		
		return result;
		
	}
	
	public static void loadResultToHive(Path path) throws SQLException{
		String hql = "load data inpath '"+path.toString()+"/part*' overwrite into table "+Constants.TFIDFTABLE;
		HiveOperator.loadDataToHiveTable(hql);
		
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, Integer> words = new HashMap<String, Integer>();
		private DecimalFormat df = Constants.df;
		private String uid, word;
		private int wc, docs;
		private Double tf, idf;
		private String[] kv;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path in = new Path(PropertiesUtils.getRootDir() + Constants.TF);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br;
			String[] kv;
			String line;
			FileStatus[] files = fs.listStatus(in, new PathFilter() {
				@Override
				public boolean accept(Path path) {
					String name = path.getName();
					return name.startsWith("part") && !name.endsWith(".crc");
				}
			});

			for (FileStatus file : files) {
				if (file.isFile()) {
					br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					line = br.readLine();
					while (line != null) {
						kv = line.split(",");
						words.put(kv[0], Integer.parseInt(kv[1]));
						line = br.readLine();
					}
					br.close();
				}

			}

			docs = HdfsUtil.readInt(new Path(PropertiesUtils.getRootDir()
					+ Constants.USERCOUNT), context.getConfiguration());
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			kv=value.toString().split(",");
			if(kv.length==4){
				uid = kv[0];
				word =kv[1];
				wc = Integer.parseInt(kv[2]);
				tf = Double.parseDouble(kv[3]);
				idf = Math.log(new Double(docs) / new Double(words.get(word)==null?1:words.get(word)+1));
				context.write(new Text(uid),new Text(word + "," + wc + "," + df.format(tf)+ "," + df.format(idf) + "," + df.format(tf*idf)));
			}									
		}

	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		double sum = 0D;
		Map<String,KeyWord> words = new HashMap<String,KeyWord>();
		double tf,idf,tfidf;
		String[] kv;
		String uid,word;
		int wc;
		KeyWord kw;
		Iterator<Entry<String, KeyWord>> ite;
		Entry<String, KeyWord> entry;
		DecimalFormat df = Constants.df;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			sum=0;
			words.clear();
			word = key.toString();
			uid = key.toString();
			for(Text v:values){
				kv = v.toString().split(",");
				if(kv.length==5){
					word = kv[0];
					wc = Integer.parseInt(kv[1]);
					tf = Double.parseDouble(kv[2]);
					idf = Double.parseDouble(kv[3]);
					tfidf = Double.parseDouble(kv[4]);
					kw = new KeyWord(word,wc,tf,idf,tfidf);
					sum += tfidf;
					words.put(word, kw);
				}				
			}
			
			ite= words.entrySet().iterator();
			
			while(ite.hasNext()){
				entry = ite.next();
				context.write(new Text(uid+","+entry.getValue().getWord()+","+entry.getValue().getWc()+","+df.format(entry.getValue().getTf())
						+","+df.format(entry.getValue().getIdf())+","+df.format(entry.getValue().getTfidf()/sum)), null);
			}
			
			
			
		}
		
				
	}

}
