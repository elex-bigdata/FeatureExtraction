package com.elex.ssp.workflow;

import java.sql.SQLException;

import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class ExportJob {
	

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		doJob();
	}
	
	public static int doJob() throws SQLException{
		int result = 0;
		result += ExportJob.featureExport();
		result += ExportJob.profileExprot();
		//result += ExportJob.userExport();
		result += userKeywordExport();
		return result;
	}
	
	public static int featureExport() throws SQLException{
		String preHql = "INSERT OVERWRITE table feature_export ";
		String hql = preHql+" select * " +
				" from feature_merge  where fv is not null " + new Condition().createExportConditionSent("featureMerge");
		System.out.println("==================featureExport-sql==================");
		System.out.println(hql);
		System.out.println("==================featureExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
	
	
	public static int profileExprot() throws SQLException{
		String preHql = "INSERT OVERWRITE table profile_export ";
		String hql = preHql+" select * " +
				" from profile_merge  where fv is not null and uid is not null and ft !='keyword' " + 
				new Condition().createExportConditionSent("profileMerge");
		System.out.println("==================profileExprot-sql==================");
		System.out.println(hql);
		System.out.println("==================profileExprot-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
		
	}
	
	public static int userExport() throws SQLException{
		String preHql = "INSERT OVERWRITE table user_export ";
		String hql = preHql+" select * " +
				" from user_merge  where uid is not null " + new Condition().createExportConditionSent("userMerge");
		System.out.println("==================userExport-sql==================");
		System.out.println(hql);
		System.out.println("==================userExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
	public static int userKeywordExport() throws SQLException{
	    String preHql = "INSERT OVERWRITE table user_keyword_export ";
		String hql = preHql+" select t.uid,'keyword',t.word," +
				" CASE WHEN p.nation IS NULL THEN 'br' ELSE p.nation END,p.pv,p.sv,p.impr,p.click,t.wc,t.tf,t.idf,t.tfidf " +
				" from tfidf t left outer join " +
				" (SELECT uid,fv,MAX(nation) AS nation,MAX(pv) AS pv,MAX(sv) AS sv,MAX(impr) AS impr,MAX(click) AS click FROM profile_merge" +
				" WHERE fv IS NOT NULL  AND uid IS NOT NULL AND ft == 'keyword' "+ new Condition().createExportConditionSent("userKeywordMerge")+" GROUP BY uid,fv)p" +
				" on p.uid=t.uid and p.fv=t.word " +
				" where length(t.word)>2 and length(t.word)<1000 ";
		System.out.println("==================userKeywordExport-sql==================");
		System.out.println(hql);
		System.out.println("==================userKeywordExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
}
