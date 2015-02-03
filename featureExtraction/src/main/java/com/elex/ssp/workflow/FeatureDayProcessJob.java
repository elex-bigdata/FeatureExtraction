package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class FeatureDayProcessJob extends Job {

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws SQLException, ParseException {
		
		FeatureDayProcessJob job = new FeatureDayProcessJob();
		
		boolean isInit = PropertiesUtils.getIsInit();
		
		job.process(isInit);
		
	}
	
	public int doJob(String day) throws SQLException{
		int result = 0;
		result += FeatureDayProcessJob.timeFeature(day);
		result += FeatureDayProcessJob.IPFeature(day);
		result += FeatureDayProcessJob.browserFeature(day);
		result += FeatureDayProcessJob.userFeature(day);
		result += FeatureDayProcessJob.projectFeature(day);
		result += FeatureDayProcessJob.queryFeature(day);
		result += FeatureDayProcessJob.queryLengthFeature(day);
		result += FeatureDayProcessJob.queryWordCountFeature(day);
		result += FeatureDayProcessJob.keywordFeature(day);
		result += FeatureDayProcessJob.refFeature(day);
		result += FeatureDayProcessJob.optFeature(day);
		return result;
	}
	
	public static int refFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='ref') ";
		String hql = preHql+" select ref,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and ref is not null  and ref !='' and nation is not null and adid is not null and dt is not null " +
				" group by ref,nation,adid,dt";
		System.out.println("==================DayProcess-refFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-refFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;

	}
	
	public static int optFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='opt') ";
		String hql = preHql+" select opt,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and opt is not null  and opt !='' and nation is not null and adid is not null and dt is not null " +
				" group by opt,nation,adid,dt";
		System.out.println("==================DayProcess-optFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-optFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;

	}
	
	public static int timeFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION tf as 'com.elex.ssp.udf.TimeDim'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='time') ";
		String hql = preHql+" select tab.col1,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge lateral view tf(time,nation) tab as col1 " +
				"where day ='"+day+"' and time is not null and nation is not null and adid is not null and dt is not null " +
				" group by tab.col1,nation,adid,dt";
		System.out.println("==================DayProcess-timeFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-timeFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;

	}
	
	public static int IPFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION area AS 'com.elex.ssp.udf.IPDim'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='area') ";
		String hql = preHql+" select area(ip),nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and size(split(ip, '\\\\.')) >= 2 and nation is not null and adid is not null and dt is not null " +
				" group by area(ip),nation,adid,dt";
		System.out.println("==================DayProcess-IPFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-IPFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int browserFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='browser') ";
		String hql = preHql+" select ua,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and ua is not null and nation is not null and adid is not null and dt is not null " +
				" group by ua,nation,adid,dt";
		System.out.println("==================DayProcess-browserFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-browserFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int userFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='user') ";
		String hql = preHql+" select uid,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and uid is not null and nation is not null and adid is not null and dt is not null " +
				" group by uid,nation,adid,dt";
		System.out.println("==================DayProcess-userFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-userFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int projectFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='project') ";
		String hql = preHql+" select pid,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from log_merge " +
				"where day ='"+day+"' and pid is not null and nation is not null and adid is not null and dt is not null " +
				" group by pid,nation,adid,dt";
		System.out.println("==================DayProcess-projectFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-projectFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='query') ";
		String hql = preHql+" select qn(query),nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from query_en " +
				"where day ='"+day+"' and query is not null and nation is not null and adid is not null and dt is not null " +
				" group by qn(query),nation,adid,dt";
		System.out.println("==================DayProcess-queryFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryLengthFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION ql AS 'com.elex.ssp.udf.QueryLength'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='query_length') ";
		String hql = preHql+" select ql(query),nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from query_en " +
				"where day ='"+day+"' and query is not null and nation is not null and adid is not null and dt is not null " +
				" group by ql(query),nation,adid,dt";
		System.out.println("==================DayProcess-queryLengthFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryLengthFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryWordCountFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION wc AS 'com.elex.ssp.udf.WordCount'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='query_word_count') ";
		String hql = preHql+" select wc(query),nation,adid,sum(pv),sum(sv),sum(impr),sum(click),dt " +
				"from query_en " +
				"where day ='"+day+"' and query is not null and nation is not null and adid is not null and dt is not null " +
				" group by wc(query),nation,adid,dt";
		System.out.println("==================DayProcess-queryWordCountFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryWordCountFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int keywordFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION sed as 'com.elex.ssp.udf.KeyWord'");
		String preHql = "insert overwrite table feature partition(day='"+day+"',ft='keyword') ";
		String hql = preHql+" select k.keyword,k.nation,k.adid,sum(k.pv),sum(k.sv),sum(k.impr),sum(k.click),k.dt from " +
				"(select tab.col1 as keyword,nation,adid,pv,sv,impr,click,dt from query_en lateral view sed(query) tab as col1 " +
				"where day ='"+day+"' and query is not null and nation is not null and adid is not null and dt is not null)k group by k.keyword,k.nation,k.adid,k.dt";
		System.out.println("==================DayProcess-keywordFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-keywordFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	

}
