package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.feature.tfidf.IDF;
import com.elex.ssp.feature.tfidf.TF;


public class PrepareJob {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		
	}
	
	public static int prepare(boolean isInit,String day) throws SQLException{
		
		String yestoday = Constants.getYestoday();
		if(isInit){
			
		}
		int result = logMerge(day);
		result += queryEnCollect(day);
		return result;
	}

	public static int logMerge(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		String preHql = " insert overwrite table log_merge partition(day='"+day+"') ";
		String navHql = " (SELECT reqid,MAX(uid) as uid,MAX(pid) as pid,MAX(ip) as ip,MAX(nation) as nation,MAX(ua) as ua,MAX(os) as os,MAX(width) as width,MAX(height) as height,COUNT(1) AS pv  FROM nav_visit WHERE DAY = '"+day+"' GROUP BY reqid )a ";
		String imprHql = " (SELECT reqid,adid,MAX(time)as time,COUNT(uid) AS impr FROM ad_impression WHERE DAY='"+day+"' GROUP BY reqid,adid)b ";
		String clickHql = " (SELECT reqid,COUNT(1) AS click FROM ad_click WHERE DAY ='"+day+"' GROUP BY reqid)c ";
		String searchHql = " (SELECT reqid,CONCAT_WS(':',collect_set(qn(keyword))) AS q,COUNT(uid) AS sv FROM search WHERE DAY='"+day+"' GROUP BY reqid)d ";
		String hql = preHql+"SELECT b.reqid,a.uid,a.pid,a.ip,a.nation,a.ua,a.os,a.width,a.height,a.pv,b.adid,b.impr,b.time,c.click,d.q,d.sv FROM "+imprHql+"LEFT OUTER JOIN "+navHql+"ON a.reqid = b.reqid LEFT OUTER JOIN "+clickHql+"ON c.reqid = b.reqid LEFT OUTER JOIN "+searchHql+"ON d.reqid = b.reqid";
		System.out.println("==================PrepareJob-logMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-logMerge-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	public static int queryEnCollect(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qs AS 'com.elex.ssp.udf.QuerySplit'");
		String preHql = " insert overwrite table query_en partition(day='"+day+"') ";
		String hql = preHql+" select reqid,uid,pid,tab.col1,nation,adid,pv,impr,sv,click from log_merge lateral view qs(query,':') tab as col1 " +
				"where day ='"+day+"' and array_contains(array('in','us','pk','ph','gb','au','za','lk','ca','sg','sg','nz','ie','ng','gh','cm'),nation) and query is not null";
		System.out.println("==================PrepareJob-queryEnCollect-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-queryEnCollect-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
}
