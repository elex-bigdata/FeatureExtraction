package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;

public class UserProfileDayProcess {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		
		process();
		
	}
	
	public static int process() throws SQLException{
		int result = 0;
		result += timeFeature();
		result += IPFeature();
		result += browserFeature();
		result += userFeature();
		result += projectFeature();
		result += queryFeature();
		result += queryLengthFeature();
		result += queryWordCountFeature();
		result += keywordFeature();
		return result;
	}
	
	public static int timeFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION tf as 'com.elex.ssp.udf.TimeDim'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='time') ";
		String hql = preHql+" select uid,tab.col1,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge lateral view tf(time,nation) tab as col1 " +
				"where day ='"+Constants.getYestoday()+"' and time is not null and nation is not null and uid is not null" +
				" group by uid,tab.col1,nation";
		System.out.println("==================DayProcess-timeFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-timeFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;

	}
	
	public static int IPFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION area AS 'com.elex.ssp.udf.IPDim'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='area') ";
		String hql = preHql+" select uid,area(ip) as a,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge " +
				"where day ='"+Constants.getYestoday()+"' and ip is not null and uid is not null" +
				" group by uid,a,nation";
		System.out.println("==================DayProcess-IPFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-IPFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int browserFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='browser') ";
		String hql = preHql+" select uid,ua,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge " +
				"where day ='"+Constants.getYestoday()+"' and ua is not null and uid is not null" +
				" group by uid,ua,nation";
		System.out.println("==================DayProcess-browserFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-browserFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int userFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='user') ";
		String hql = preHql+" select uid,'u',nation,adid,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge " +
				"where day ='"+Constants.getYestoday()+"' and uid is not null and uid is not null" +
				" group by uid,nation";
		System.out.println("==================DayProcess-userFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-userFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int projectFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='project') ";
		String hql = preHql+" select uid,pid,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from log_merge " +
				"where day ='"+Constants.getYestoday()+"' and pid is not null and uid is not null" +
				" group by uid,pid,nation";
		System.out.println("==================DayProcess-userFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-userFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='query') ";
		String hql = preHql+" select uid,qn(query) as q,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en " +
				"where day ='"+Constants.getYestoday()+"' and query is not null and uid is not null" +
				" group by uid,q,nation";
		System.out.println("==================DayProcess-queryFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryLengthFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION ql AS 'com.elex.ssp.udf.QueryLength'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='query_length') ";
		String hql = preHql+" select uid,ql(query) as ql,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en " +
				"where day ='"+Constants.getYestoday()+"' and query is not null and uid is not null" +
				" group by uid,ql,nation";
		System.out.println("==================DayProcess-queryLengthFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryLengthFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryWordCountFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION wc AS 'com.elex.ssp.udf.WordCount'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='query_length') ";
		String hql = preHql+" select uid,wc(query) as wc,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en " +
				"where day ='"+Constants.getYestoday()+"' and query is not null and uid is not null" +
				" group by uid,wc,nation";
		System.out.println("==================DayProcess-queryWordCountFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-queryWordCountFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int keywordFeature() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION sed as 'com.elex.ssp.udf.KeyWord'");
		String preHql = "insert overwrite table profile partition(day='"+Constants.getYestoday()+"',ft='query_length') ";
		String hql = preHql+" select uid,tab.col1 as keyword,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from query_en lateral view sed(query) tab as col1 " +
				"where day ='"+Constants.getYestoday()+"' and query is not null and uid is not null" +
				" group uid,keyword,nation";
		System.out.println("==================DayProcess-keywordFeature-sql==================");
		System.out.println(hql);
		System.out.println("==================DayProcess-keywordFeature-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	

}
