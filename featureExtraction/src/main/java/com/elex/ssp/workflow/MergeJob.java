package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;

public class MergeJob {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		doJob();
	}
	
	public static int doJob() throws SQLException{
		int result = 0;
		result += MergeJob.featureMerge();
		result += MergeJob.profileMerge();
		result += MergeJob.userMerge();
		return result;
	}
	
	public static int featureMerge() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature_merge ";
		String hql = preHql+" select ft,fv,nation,adid,sum(pv),sum(sv),sum(impr),sum(click),round(sum(click)/sum(pv),4),round(sum(click)/sum(impr),4),round(sum(impr)/sum(pv),4) " +
				" from feature  " +
				" where day >'"+Constants.getStartDay()+"' and fv is not null " +
				" group by ft,fv,nation,adid";
		System.out.println("==================featureMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================featureMerge-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int profileMerge() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		
		/*String preHql = "insert overwrite table profile_merge ";
		String kwSql = preHql+" select a.*,b.tfidf from (select uid,ft,fv,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				" from profile  " +
				" where day >'"+Constants.getStartDay()+"' and fv is not null and ft='keyword' " +
				" group ft,fv,nation)a left outer join tfidf b on a.uid=b.uid and a.fv=b.word";
		System.out.println("==================profileMerge-keyword-sql==================");
		System.out.println(kwSql);
		System.out.println("==================profileMerge-keyword-sql==================");
		stmt.execute(kwSql);*/
		
		String hql = "insert into table profile_merge  select uid,ft,fv,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				" from profile  " +
				" where day >'"+Constants.getStartDay()+"' and fv is not null " +
				" group by uid,ft,fv,nation";
		System.out.println("==================profileMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================profileMerge-sql==================");
		stmt.execute(hql);
		
		stmt.close();
		return 0;
		
	}
	
	public static int userMerge() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table user_merge ";
		String hql = preHql+" select fv,nation,adid,sum(pv),sum(impr),sum(sv),sum(click) " +
				" from feature  " +
				" where ft ='user' and fv is not null " +
				" group by fv,nation,adid";
		System.out.println("==================userMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================userMerge-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}

}
