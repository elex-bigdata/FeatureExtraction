package com.elex.ssp.scheduler;

import java.sql.SQLException;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;


public class PrepareJob {

	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException {
		prepare();
	}

	public static void prepare() throws SQLException{
		String preHql = " insert overwrite table log_merge partition(day='"+Constants.getToday()+"') ";
		String navHql = " (SELECT reqid,MAX(uid) as uid,MAX(pid) as pid,MAX(ip) as ip,MAX(nation) as nation,MAX(ua) as ua,MAX(os) as os,MAX(width) as width,MAX(height) as height,COUNT(1) AS pv  FROM nav_visit WHERE DAY = '"+Constants.getToday()+"' GROUP BY reqid )a ";
		String imprHql = " (SELECT reqid,adid,MAX(time)as time,COUNT(uid) AS impr FROM ad_impression WHERE DAY='"+Constants.getToday()+"' GROUP BY reqid,adid)b ";
		String clickHql = " (SELECT reqid,COUNT(1) AS click FROM ad_click WHERE DAY ='"+Constants.getToday()+"' GROUP BY reqid)c ";
		String searchHql = " (SELECT reqid,CONCAT_WS(':',collect_set(qn(keyword))) AS q,COUNT(uid) AS sv FROM search WHERE DAY='"+Constants.getToday()+"' GROUP BY reqid)d ";
		String hql = preHql+"SELECT b.reqid,a.uid,a.pid,a.ip,a.nation,a.ua,a.os,a.width,a.height,a.pv,b.adid,b.impr,b.time,c.click,d.q,d.sv FROM "+imprHql+"LEFT OUTER JOIN "+navHql+"ON a.reqid = b.reqid LEFT OUTER JOIN "+clickHql+"ON c.reqid = b.reqid LEFT OUTER JOIN "+searchHql+"ON d.reqid = b.reqid";
		System.out.print(hql);
		//HiveOperator.executeHQL(hql);
	}
}
