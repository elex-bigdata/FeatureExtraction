package com.elex.ssp.workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;


public class PrepareJob extends Job{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		boolean isInit = PropertiesUtils.getIsInit();
		PrepareJob job = new PrepareJob();
		job.process(isInit);
		
		
	}
	
	@Override	
	public int doJob(String day) throws SQLException{
		int result = 0;
		result += PrepareJob.logMerge(day);
		result += logMerge2(day);
		result += PrepareJob.queryEnCollect(day);
		result += queryEnCollect2(day);
		result += gdpGoogleSearch(day);
		result += gdpODP(day);
		result += gdpUTag(day);
		return result;
	}
	

	/**
	 * 按reqid拼接日志，以广告展现表左关联nv表获取用户属性，结果再左关联点击表获取点击数据，结果再左关联搜索表获取搜索词。
	 * 因为有passback，所以需要自定义函数来剪除多记录的impr。主流量的adid和tag要在fecomm项目中配置。
	 * 如果一个reqid（请求）有多个广告（adid），展现表的汇总结果需要加上slot的group条件。click需要新增adid字段，同时click表的汇总结果需要加上adid的group条件。
	 * 展现表用reqid与nv表左关联，结果再用reqid和adid与click表左关联，之后如搜索表的左关联也如此。
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int logMerge(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		stmt.execute("CREATE TEMPORARY FUNCTION concatcolon AS 'com.elex.ssp.udf.GroupConcatColon'");
		stmt.execute("CREATE TEMPORARY FUNCTION cadid AS 'com.elex.ssp.udf.ChooseAdid'");
		stmt.execute("CREATE TEMPORARY FUNCTION cdt AS 'com.elex.ssp.udf.ChooseDt'");
		String preHql = " insert overwrite table log_merge partition(day='"+day+"') ";
		String navHql = " (SELECT reqid,MAX(regexp_replace(uid,',','')) as uid,MAX(pid) as pid,MAX(ip) as ip,MAX(nation) " +
				        "as nation,MAX(ua) as ua,MAX(os) as os,MAX(width) as width,MAX(height) as height,1 AS pv,max(ref) as ref," +
				        "max(opt) as opt  FROM nav_visit WHERE DAY = '"+day+"' GROUP BY reqid )a ";
		String imprHql = " (SELECT reqid,cadid(adid) AS adid,cdt(dt) AS ndt,1 AS impr,MAX(TIME) AS TIME FROM ad_impression" +
				         " WHERE DAY = '"+day+"' GROUP BY reqid,slot)b ";
		String clickHql = " (SELECT reqid,COUNT(1) AS click FROM ad_click WHERE DAY ='"+day+"' GROUP BY reqid)c ";
		String searchHql = " (SELECT reqid,concatcolon(qn(keyword)) AS q,COUNT(uid) AS sv FROM search WHERE DAY='"+day+"' GROUP BY reqid)d ";
		String hql = preHql+"SELECT b.reqid,a.uid,a.pid,a.ip,a.nation,a.ua,a.os,a.width,a.height,case when a.pv is null then 0 else a.pv end," +
				"b.adid,case when b.impr is null then 0 else b.impr end,b.time,case when c.click is null then 0 else c.click end," +
				"d.q,case when d.sv is null then 0 else d.sv end,CASE WHEN b.ndt IS NULL THEN 'default' ELSE b.ndt END,a.ref,a.opt" +
				" FROM "+imprHql+"LEFT OUTER JOIN "+navHql+"ON a.reqid = b.reqid LEFT OUTER JOIN "+clickHql+"ON c.reqid = b.reqid" +
				" LEFT OUTER JOIN "+searchHql+"ON d.reqid = b.reqid";
		System.out.println("==================PrepareJob-logMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-logMerge-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	
	/**
	 * 按reqid合并日志，在log_merge表的基础上将一个请求的多个adid合并为一条。
	 * 便于user-profile的生成，因为user-profile是以uid作为统计维度，不再关心adid。
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int logMerge2(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		String hql = "insert overwrite table log_merge2 partition(day='"+day+"') " +
				"select reqid,max(time),uid,max(pid),max(ip),max(nation),max(ua),max(os),max(width),max(height),max(pv)," +
				"sum(impr),sum(click),max(sv),dt,max(ref),max(opt) " +
				"from log_merge where day ='"+day+"' and uid is not null group by reqid,uid,dt";
		stmt.execute(hql);
		System.out.println("==================PrepareJob-logMerge2-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-logMerge2-sql==================");
		stmt.close();
		return 0;		
	}
	
	/**
	 * 将log_merge表中的搜索日志拆分，之前是使用：合并多条。便于后续的query、word_count、query_length特征的提取。
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int queryEnCollect(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qs AS 'com.elex.ssp.udf.QuerySplit'");
		String preHql = " insert overwrite table query_en partition(day='"+day+"') ";
		String hql = preHql+" select reqid,uid,tab.col1,nation,adid,pv,impr,sv,click,dt from log_merge lateral view qs(query,':') tab as col1 " +
				" where day ='"+day+"' and array_contains(array("+PropertiesUtils.getNations()+"),nation)" +
				" and query is not null and nation is not null and uid is not null";
		System.out.println("==================PrepareJob-queryEnCollect-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-queryEnCollect-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	/**
	 * 拆分log_merge表的搜索日志，但将一个reqid含有多个adid的情况按reqid和query合并，也是便于user-profile的生成，
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int queryEnCollect2(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qs AS 'com.elex.ssp.udf.QuerySplit'");
		String preHql = " insert overwrite table query_en2 partition(day='"+day+"') ";
		String hql = preHql+" select reqid,uid,tab.col1,nation,max(pv),sum(impr),max(sv),sum(click),dt" +
				" from log_merge lateral view qs(query,':') tab as col1 " +
				" where day ='"+day+"' and array_contains(array("+PropertiesUtils.getNations()+"),nation) and query is not null" +
				" and nation is not null and uid is not null " +
				" group by reqid,uid,tab.col1,nation,dt";
		System.out.println("==================PrepareJob-queryEnCollect2-sql==================");
		System.out.println(hql);
		System.out.println("==================PrepareJob-queryEnCollect2-sql==================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	/**
	 * 提取gdp中的google搜索关键词
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int gdpGoogleSearch(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION concatspace AS 'com.elex.ssp.udf.GroupConcatSpace'");
		stmt.execute("CREATE TEMPORARY FUNCTION qn AS 'com.elex.ssp.udf.Query'");
		String hql = "INSERT OVERWRITE TABLE gdp_daysearch  partition(day='"+day+"')  SELECT a.uid,concatspace(a.q) " +
				" FROM(SELECT uid,CASE WHEN url RLIKE '.*q=(.*?)(\\&\\.*)' THEN qn(regexp_extract(url, '.*q=(.*?)(\\&\\.*)', 1)) " +
				" ELSE qn(regexp_extract(url, '.*q=(.*)', 1)) END AS q " +
				" FROM odin.gdp WHERE url LIKE '%google.com%'  AND url LIKE '%q=%'  AND DAY = " +
				" '"+day+"'  AND array_contains (array ("+PropertiesUtils.getNations()+"), nation)) a GROUP BY a.uid";
		System.out.println("=================PrepareJob-gdpGoogleSearch-sql===================");
		System.out.println(hql);
		System.out.println("=================PrepareJob-gdpGoogleSearch-sql===================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	
	/**
	 * 提取gdp行为的odp分类，同时统计每个用户对每个域名的访问次数。
	 * 注意RLIKE和split中的正则表达式
	 * parse_url (regexp_replace(url,',',''), 'HOST')
	 */
	public static int gdpODP(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION get_domain AS 'com.elex.ssp.udf.Domain'");
		String hql ="INSERT OVERWRITE TABLE odin.gdp_odp PARTITION (DAY='"+day+"') " +
				"SELECT t.uid,t.nation,t.domain,o.category,t.visit FROM odin.odp o " +
				"RIGHT OUTER JOIN (SELECT a.uid,a.nation,a.domain,COUNT(1) AS visit FROM(SELECT regexp_replace(uid,',','') AS uid," +
				"regexp_replace(nation,',','') AS nation, get_domain(url) AS domain FROM odin.gdp  " +
				"WHERE DAY='"+day+"') a  WHERE a.domain IS NOT NULL  AND NOT(a.domain RLIKE '\\\\d+\\\\.\\\\d+\\\\.\\\\d+\\\\.\\\\d+')  " +
				"AND size (split (a.domain, '\\\\.')) >= 2  GROUP BY a.uid,a.nation,a.domain) t  ON t.domain = o.host";
		System.out.println("=================PrepareJob-gdpODP-sql===================");
		System.out.println(hql);
		System.out.println("=================PrepareJob-gdpODP-sql===================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
	/**
	 * 如果一个gdp行为对应多个odp分类，将其拆分、便于后续统计，而且只保留了具有odp分类的行为。
	 * @param day
	 * @return
	 * @throws SQLException
	 */
	public static int gdpUTag(String day) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute("add jar " + Constants.UDFJAR);
		stmt.execute("CREATE TEMPORARY FUNCTION qs AS 'com.elex.ssp.udf.QuerySplit'");
		String hql ="INSERT overwrite TABLE odin.gdp_utag PARTITION (DAY='"+day+"') " +
				"SELECT uid,nation,tab.col1,SUM(visit) FROM odin.gdp_odp " +
				"lateral VIEW qs (category, ':') tab AS col1 WHERE " +
				"category IS NOT NULL AND day='"+day+"' GROUP BY uid,nation,tab.col1";
		System.out.println("=================PrepareJob-gdpUTag-sql===================");
		System.out.println(hql);
		System.out.println("=================PrepareJob-gdpUTag-sql===================");
		stmt.execute(hql);
		stmt.close();
		return 0;
	}
	
}
