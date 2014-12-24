package com.elex.ssp.workflow;

import java.sql.SQLException;

import com.elex.ssp.common.HiveOperator;

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
		result += gdpUserKeywordExport();
		result += sspUserKeywordExport();
		result += odpTagExport();
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
	

	public static int gdpUserKeywordExport() throws SQLException{
	    String preHql = "INSERT OVERWRITE table user_keyword_export ";
		String hql = preHql+" select * from (select t.uid,concat(t.source,'keyword') as ft,t.word as fv," +
				" p.nation,p.pv,p.sv,p.impr,p.click,t.wc,t.tf,t.idf,t.tfidf " +
				" from (select * from tfidf where source='gdp') t join " +
				" (SELECT fv,MAX(nation) as nation,SUM(pv) AS pv,SUM(sv) AS sv,SUM(impr) AS impr,SUM(click) AS click FROM feature_merge" +
				" WHERE fv IS NOT NULL AND ft = 'keyword'  GROUP BY fv)p" +
				" on p.fv=t.word)c " +
				" where c.uid is not null "+ new Condition().createExportConditionSent("userKeywordMerge");
		System.out.println("==================gdpuserKeywordExport-sql==================");
		System.out.println(hql);
		System.out.println("==================gdpuserKeywordExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
	
	public static int sspUserKeywordExport() throws SQLException{
	    String preHql = "INSERT INTO table user_keyword_export ";
		String hql = preHql+" select * from(select t.uid,concat(t.source,'keyword') as ft,t.word as fv," +
				" p.nation,p.pv,p.sv,p.impr,p.click,t.wc,t.tf,t.idf,t.tfidf " +
				" from (select * from tfidf where source='ssp') t join " +
				" (SELECT uid,fv,MAX(nation) as nation,SUM(pv) AS pv,SUM(sv) AS sv,SUM(impr) AS impr,SUM(click) AS click FROM profile_merge" +
				" WHERE fv IS NOT NULL  AND uid IS NOT NULL AND ft = 'keyword' GROUP BY uid,fv)p" +
				" on p.uid=t.uid and p.fv=t.word)c " +
				" where c.uid is not null "+ new Condition().createExportConditionSent("userKeywordMerge");
		System.out.println("==================sspuserKeywordExport-sql==================");
		System.out.println(hql);
		System.out.println("==================sspuserKeywordExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
	public static int odpTagExport() throws SQLException{
	    String preHql = "INSERT INTO table user_keyword_export ";
		String hql = preHql+" select * from (select t.uid,t.source,t.word as fv,p.nation,p.pv,p.sv,p.impr,p.click,t.wc,t.tf,t.idf,t.tfidf " +
				" from (select * from tfidf where source='odp') t join " +
				" (SELECT fv,MAX(nation) as nation,SUM(pv) AS pv,SUM(sv) AS sv,SUM(impr) AS impr,SUM(click) AS click FROM feature_merge" +
				" WHERE fv IS NOT NULL AND ft = 'odp'  GROUP BY fv)p" +
				" on p.fv=t.word)c " +
				" where c.uid is not null "+ new Condition().createExportConditionSent("userKeywordMerge");
		System.out.println("==================odpTagExport-sql==================");
		System.out.println(hql);
		System.out.println("==================odpTagExport-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		//return 0;
	}
	
}
