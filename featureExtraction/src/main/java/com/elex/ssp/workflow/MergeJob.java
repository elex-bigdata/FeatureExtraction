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
		result += featureMerge();
		result += profileMerge();
		return result;
	}
	
	public static int featureMerge() throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String preHql = "insert overwrite table feature_merge	 ";
		String hql = preHql+" select ft,fv,nation,adid,sum(pv) as pvs,sum(sv) as svs,sum(impr) as ims,sum(click) as cs,round(cs/pvs,4),round(cs/ims,4),round(ims/pvs,4) " +
				"from feature  " +
				"where day >'"+Constants.getStartDay()+"' and fv is not null" +
				" group ft,fv,nation,adid";
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
		String preHql = "insert overwrite table profile_merge ";
		String hql = preHql+" select uid,ft,fv,nation,sum(pv),sum(sv),sum(impr),sum(click) " +
				"from profile  " +
				"where day >'"+Constants.getStartDay()+"' and fv is not null and uid is not null" +
				" group ft,fv,nation";
		System.out.println("==================profileMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================profileMerge-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
		
	}

}
