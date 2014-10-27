package com.elex.ssp.feature.tfidf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

		Path output = new Path(PropertiesUtils.getRootDir() + Constants.IDF);
		HdfsUtil.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);

		int result = job.waitForCompletion(true) ? 0 : 1;
		
		loadResultToHive(output);
		
		return result;
		
	}
	
	public static void loadResultToHive(Path path) throws SQLException{
		String hql = "load data inpath '"+path.toString()+"' into table "+Constants.TFIDFTABLE +" partition(day='"+Constants.getToday()+"')";
		HiveOperator.loadDataToHiveTable(hql);
		
	}

	public static class MyMapper extends Mapper<Text, Text, Text, Text> {

		private Map<String, Integer> words = new HashMap<String, Integer>();
		private DecimalFormat df = Constants.df;
		private String uid, word;
		private int wc, docs;
		private Double tf, idf, tfidf;

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
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			uid = key.toString().split(",")[0];
			word = key.toString().split(",")[1];
			wc = Integer.parseInt(value.toString().split(",")[0]);
			tf = Double.parseDouble(value.toString().split(",")[1]);
			idf = Math.log(Double.valueOf(docs / words.get(word)));
			tfidf = tf * idf;
			context.write(null,new Text(uid + "," + word + "," + wc + "," + df.format(tf)+ "," + df.format(idf) + "," + df.format(tfidf)));

		}

	}

}
