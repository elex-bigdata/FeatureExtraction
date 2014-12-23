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
		result += odpFeatureMerge();
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
	
	public static int odpFeatureMerge() throws SQLException{
		String hql = "INSERT INTO TABLE feature_merge SELECT 'odp',t.tag,t.nation,t.adid,SUM(t.pv),SUM(t.sv),SUM(t.impr),SUM(t.click)," +
				" ROUND(CASE WHEN SUM(t.click) IS NULL OR SUM(t.pv) IS NULL THEN 0 ELSE SUM(t.click)/SUM(t.pv) END,4)," +
				" ROUND(CASE WHEN SUM(t.click) IS NULL OR SUM(t.pv) IS NULL THEN 0 ELSE SUM(t.click)/SUM(t.pv) END,4)," +
				" ROUND(CASE WHEN SUM(t.impr) IS NULL OR SUM(t.pv) IS NULL THEN 0 ELSE SUM(t.impr)/SUM(t.pv) END,4) " +
				" FROM(SELECT /*+ MAPJOIN(b) */ b.tag,a.* FROM user_tag b JOIN " +
				" (SELECT fv,nation,adid,pv,sv,impr,click FROM feature_merge WHERE ft='user')a  ON a.fv=b.uid)t GROUP BY t.tag,t.nation,t.adid";
		System.out.println("==================odpFeatureMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================odpFeatureMerge-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
	}
	
				

}
