package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class UserProfileDayProcessJob extends Job{

	/**
	 * @param args
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws SQLException, ParseException {
		
		UserProfileDayProcessJob job = new UserProfileDayProcessJob();
		
		boolean isInit = PropertiesUtils.getIsInit();
		
		job.process(isInit);
	}
	
	
	public int doJob(String day) throws SQLException{
		int result = 0;
		result += UserProfileDayProcessJob.timeFeature(day);
		result += UserProfileDayProcessJob.IPFeature(day);
		result += UserProfileDayProcessJob.browserFeature(day);
		result += UserProfileDayProcessJob.userFeature(day);
		result += UserProfileDayProcessJob.projectFeature(day);
		result += UserProfileDayProcessJob.queryFeature(day);
		result += UserProfileDayProcessJob.queryLengthFeature(day);
		result += UserProfileDayProcessJob.queryWordCountFeature(day);
		result += UserProfileDayProcessJob.keywordFeature(day);
		return result;
	}
	
	public static int timeFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION tf as 'com.elex.ssp.udf.TimeDim'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='time') ";
		String hql = preHql+" select uid,tab.col1,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge2 lateral view tf(time,nation) tab as col1 " +
				"where day ='"+day+"' and time is not null and nation is not null and uid is not null " +
				" group by uid,tab.col1,nation";
		System.out.println("==================profileDayProcess-timeFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-timeFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;

	}
	
	public static int IPFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION area AS 'com.elex.ssp.udf.IPDim'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='area') ";
		String hql = preHql+" select uid,area(ip),nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge2 " +
				"where day ='"+day+"' and ip is not null and uid is not null and nation is not null" +
				" group by uid,area(ip),nation";
		System.out.println("==================profileDayProcess-IPFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-IPFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int browserFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='browser') ";
		String hql = preHql+" select uid,ua,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge2 " +
				"where day ='"+day+"' and ua is not null and uid is not null and nation is not null " +
				" group by uid,ua,nation";
		System.out.println("==================profileDayProcess-browserFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-browserFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int userFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='user') ";
		String hql = preHql+" select uid,uid,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge2 " +
				"where day ='"+day+"' and uid is not null and nation is not null" +
				" group by uid,nation";
		System.out.println("==================profileDayProcess-userFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-userFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int projectFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='project') ";
		String hql = preHql+" select uid,pid,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge2 " +
				"where day ='"+day+"' and pid is not null and uid is not null and nation is not null " +
				" group by uid,pid,nation";
		System.out.println("==================profileDayProcess-projectFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-projectFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='query') ";
		String hql = preHql+" select uid,qn(query),nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en2 " +
				"where day ='"+day+"' and query is not null and uid is not null and nation is not null " +
				" group by uid,qn(query),nation";
		System.out.println("==================profileDayProcess-queryFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-queryFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryLengthFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION ql AS 'com.elex.ssp.udf.QueryLength'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='query_length') ";
		String hql = preHql+" select uid,ql(query),nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en2 " +
				"where day ='"+day+"' and query is not null and uid is not null and nation is not null " +
				" group by uid,ql(query),nation";
		System.out.println("==================profileDayProcess-queryLengthFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-queryLengthFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryWordCountFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION wc AS 'com.elex.ssp.udf.WordCount'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='query_word_count') ";
		String hql = preHql+" select uid,wc(query),nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en2 " +
				"where day ='"+day+"' and query is not null and uid is not null and nation is not null " +
				" group by uid,wc(query),nation";
		System.out.println("==================profileDayProcess-queryWordCountFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-queryWordCountFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int keywordFeature(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION sed as 'com.elex.ssp.udf.KeyWord'");
		String preHql = "insert overwrite table profile partition(day='"+day+"',ft='keyword') ";
		String hql = preHql+" select uid,k.keyword,k.nation,sum(pv),sum(sv),sum(impr),sum(click) from " +
				"(select uid,tab.col1 as keyword,nation,pv,sv,impr,click from query_en2 lateral view sed(query) tab as col1 " +
				"where day ='"+day+"' and query is not null and uid is not null and nation is not null)k group by k.uid,k.keyword,k.nation";
		System.out.println("==================profileDayProcess-keywordFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================profileDayProcess-keywordFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	

}
