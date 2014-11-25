package com.elex.ssp.workflow;

import java.sql.SQLException;

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
		String preHql = "insert overwrite table feature_merge ";
		String hql = preHql+" select ft,fv,nation,adid,sum(pv),sum(sv),sum(impr),sum(click)," +
				"round(case when sum(click) is null or sum(pv) is null then 0 else sum(click)/sum(pv) end,4)," +
				"round(case when sum(click) is null or sum(impr) is null then 0 else sum(click)/sum(impr) end,4)," +
				"round(case when sum(impr) is null or sum(pv) is null then 0 else sum(impr)/sum(pv) end,4) " +
				" from feature  " +
				" where day >'"+Constants.getStartDay()+"' and fv is not null and dt is not null " +
				" group by ft,fv,nation,adid";
		System.out.println("==================featureMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================featureMerge-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
	}
	
	public static int profileMerge() throws SQLException{
		String hql = "insert overwrite table profile_merge  select uid,ft,fv,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				" from profile  " +
				" where day >'"+Constants.getStartDay()+"' and fv is not null and dt is not null " +
				" group by uid,ft,fv,nation";
		System.out.println("==================profileMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================profileMerge-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
		
	}
	
	public static int userMerge() throws SQLException{
		String preHql = "insert overwrite table user_merge ";
		String hql = preHql+" select fv,nation,sum(pv),sum(impr),sum(sv),sum(click) " +
				" from profile  " +
				" where ft ='user' and fv is not null and dt is not null " +
				" group by fv,nation";
		System.out.println("==================userMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================userMerge-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
	}

}
