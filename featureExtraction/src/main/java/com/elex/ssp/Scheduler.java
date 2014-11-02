package com.elex.ssp;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;
import com.elex.ssp.feature.tfidf.IDF;
import com.elex.ssp.feature.tfidf.TF;
import com.elex.ssp.workflow.ExportJob;
import com.elex.ssp.workflow.FeatureDayProcessJob;
import com.elex.ssp.workflow.MergeJob;
import com.elex.ssp.workflow.PrepareJob;
import com.elex.ssp.workflow.UserProfileDayProcessJob;

public class Scheduler {
	
	private static final Logger log = LoggerFactory.getLogger(Scheduler.class);

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = { otherArgs[0], otherArgs[1] };// 运行阶段控制参数
		int success = 0;
		
		// stage 0
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("create tables!!!");
			success = createTables();
			if (success != 0) {
				log.error("create tables ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("create tables SUCCESS!!!");
		}
		
		// stage 1
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("prepare input !!!");
			success = prepare();
			if (success != 0) {
				log.error("prepare inputs ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("prepare input SUCCESS!!!");
		}
		
		// stage 2
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("feature day process !!!");
			success = featureDayProcess();
			if (success != 0) {
				log.error("feature day process ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("feature day process SUCCESS!!!");
		}
		
		// stage 3
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("profile day process !!!");
			success = profileDayProcess();
			if (success != 0) {
				log.error("profile day process ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("profile day process SUCCESS!!!");
		}
		
		
		// stage 4
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("tfidf !!!");
			success = tfidf();
			if (success != 0) {
				log.error("tfidf ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("tfidf SUCCESS!!!");
		}
		
		
		// stage 5
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("merge !!!");
			success = merge();
			if (success != 0) {
				log.error("merge ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("merge SUCCESS!!!");
		}
		
		// stage 6
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("export !!!");
			success = export();
			if (success != 0) {
				log.error("export ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("export SUCCESS!!!");
		}
		
		
		HiveOperator.closeConn();
	}
	
	public static int createTables() throws SQLException{
		
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		
		String logMerge ="create table IF NOT EXISTS log_merge(reqid string,uid string,pid string,ip string,nation string,ua string,os string,width string,height string,pv int,adid string,impr int,time string,click int,query string,sv int) partitioned by(day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";		
		stmt.execute(logMerge);
		
		String logMerge2="CREATE TABLE IF NOT EXISTS log_merge2 (reqid STRING,time STRING,uid STRING,pid STRING,ip STRING,nation STRING,ua STRING,os STRING,width STRING,height STRING,pv INT,impr INT,click INT,sv INT) partitioned BY (DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored AS textfile ";
		stmt.execute(logMerge2);
		
		String queryEn = "create table IF NOT EXISTS query_en(reqid string,uid string,query string,nation string,adid string,pv int,impr int,sv int,click int) partitioned by(day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(queryEn);
		
		String queryEn2 = "create table IF NOT EXISTS query_en2(reqid string,uid string,query string,nation string,pv int,impr int,sv int,click int) partitioned by(day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(queryEn2);
		
		String tfidf="create table IF NOT EXISTS tfidf(uid string,word string,wc int,tf double,idf double,tfidf double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(tfidf);
		
		String feature ="create table IF NOT EXISTS feature(fv string,nation string,adid string,pv int,sv int,impr int,click int) partitioned by(day string,ft string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(feature);
		
		String profile="create table IF NOT EXISTS profile(uid string,fv string,nation string,pv int,sv int,impr int,click int) partitioned by(day string,ft string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(profile);
		
		String featureMerge ="create table IF NOT EXISTS feature_merge(ft string,fv string,nation string,adid string,pv int,sv int,impr int,click int,ctr1 double,ctr2 double,ad_fill double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(featureMerge);
		
		String profileMerge ="create table IF NOT EXISTS profile_merge(uid string,ft string,fv string,nation string,pv int,sv int,impr int,click int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(profileMerge);
		
		String userMerge ="create table IF NOT EXISTS user_merge(uid string,nation string,adid string,pv int,impr int,sv int,click int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(userMerge);
		
		String exprotFeature ="create table IF NOT EXISTS feature_export(ft string,fv string,nation string,adid string,pv int,sv int,impr int,click int,ctr1 double,ctr2 double,ad_fill double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(exprotFeature);
		
		String exportProfile ="create table IF NOT EXISTS profile_export(uid string,ft string,fv string,nation string,pv int,sv int,impr int,click int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(exportProfile);
		
		String exportUserMerge ="create table IF NOT EXISTS user_export(uid string,nation string,adid string,pv int,impr int,sv int,click int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(exportUserMerge);
		
		String exportUserKeyword ="create table IF NOT EXISTS user_keyword_export(uid string,ft string,fv string,nation string,pv int,sv int,impr int,click int,wc int, tf double,idf double,tfidf double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' stored as textfile";
		stmt.execute(exportUserKeyword);
		
		stmt.close();
		return 0;
	}
	
	public static int prepare() throws SQLException, ParseException{
		
		boolean isInit = PropertiesUtils.getIsInit();
		PrepareJob job = new PrepareJob();
		return job.process(isInit);
		
	}
	
	public static int featureDayProcess() throws SQLException, ParseException{
		int result =0;
		boolean isInit = PropertiesUtils.getIsInit();
		
		FeatureDayProcessJob featureJob = new FeatureDayProcessJob();						
		result += featureJob.process(isInit);
		
		UserProfileDayProcessJob profielJob = new UserProfileDayProcessJob();		
		result += profielJob.process(isInit);
		
		return result;		
		
	}
	
	public static int profileDayProcess() throws SQLException, ParseException{
		int result =0;
		boolean isInit = PropertiesUtils.getIsInit();
		
		UserProfileDayProcessJob profielJob = new UserProfileDayProcessJob();		
		result += profielJob.process(isInit);
		
		return result;		
		
	}
	
	public static int tfidf() throws Exception{
		int a = ToolRunner.run(new Configuration(), new TF(), null);
		int b = ToolRunner.run(new Configuration(), new IDF(), null);
		return Math.max(a, b);
	}
	
	public static int merge() throws SQLException{
		
		return MergeJob.doJob();
	}
	
   public static int export() throws SQLException{
		
		return ExportJob.doJob();
	}
	
	protected static boolean shouldRunNextPhase(String[] args, AtomicInteger currentPhase) {
	    int phase = currentPhase.getAndIncrement();
	    String startPhase = args[0];
	    String endPhase = args[1];
	    boolean phaseSkipped = (startPhase != null && phase < Integer.parseInt(startPhase))
	        || (endPhase != null && phase > Integer.parseInt(endPhase));
	    if (phaseSkipped) {
	      log.info("Skipping phase {}", phase);
	    }
	    return !phaseSkipped;
	  }

}
