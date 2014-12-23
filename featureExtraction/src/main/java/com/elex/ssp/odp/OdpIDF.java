package com.elex.ssp.odp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HdfsUtil;
import com.elex.ssp.common.PropertiesUtils;
import com.elex.ssp.feature.tfidf.KeyWord;

public class OdpIDF extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Configuration(), new OdpIDF(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "idf");
		job.setJarByClass(OdpIDF.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);

		Path in = new Path(PropertiesUtils.getRootDir() + Constants.ODPTF);
	
		FileInputFormat.addInputPath(job, in);

		job.setOutputFormatClass(TextOutputFormat.class);
		Path output = new Path(PropertiesUtils.getRootDir() + Constants.ODPTFIDF);
		HdfsUtil.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		MultipleOutputs.addNamedOutput(job, "tag", TextOutputFormat.class, Text.class, Text.class);
		
		int result = job.waitForCompletion(true) ? 0 : 1;	
		return result;
		
	}
	

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Map<String, Double> words = new HashMap<String, Double>();
		private DecimalFormat df = Constants.df;
		private String uid, word;
		private int wc;
		private Double tf, idf;
		private String[] kv;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path in = new Path(PropertiesUtils.getRootDir() + Constants.ODPIDF);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br;
			String[] kv;
			String line;
			FileStatus[] files = fs.listStatus(in, new PathFilter() {
				@Override
				public boolean accept(Path path) {
					String name = path.getName();
					return name.startsWith("00") && !name.endsWith(".crc");
				}
			});

			for (FileStatus file : files) {
				if (file.isFile()) {
					br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					line = br.readLine();
					while (line != null) {
						kv = line.split("\\x01");
						words.put(kv[0], Double.parseDouble(kv[1]));
						line = br.readLine();
					}
					br.close();
				}

			}			
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
				idf = words.get(word)==null?1:words.get(word);
				context.write(new Text(uid),new Text(word + "," + wc + "," + df.format(tf)+ "," + df.format(idf) + "," + df.format(tf*idf)));
			}									
		}

	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		double sum = 0D;
		Map<String,KeyWord> words = new HashMap<String,KeyWord>();
		double tf,idf,tfidf,max,tfidf_normalized;
		String[] kv;
		String uid,word,top;
		int wc;
		KeyWord kw;
		Iterator<Entry<String, KeyWord>> ite;
		Entry<String, KeyWord> entry;
		DecimalFormat df = Constants.df;
		private MultipleOutputs<Text, Text> tag; 
		
		@Override		
		protected void setup(Context context) throws IOException,InterruptedException {
		    tag = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			sum=0;
			max=0;
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
				tfidf_normalized = entry.getValue().getTfidf()/sum;
				context.write(new Text(uid+","+entry.getValue().getWord()+","+entry.getValue().getWc()+","+df.format(entry.getValue().getTf())
						+","+df.format(entry.getValue().getIdf())+","+df.format(tfidf_normalized)), null);
				if(tfidf_normalized>max){
					max=tfidf_normalized;
					top=entry.getKey();
				}
								
			}
			
			tag.write(new Text(uid+","+words.get(top).getWord()), null, "tag");
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException {
			tag.close();
		}
		
				
	}

}
