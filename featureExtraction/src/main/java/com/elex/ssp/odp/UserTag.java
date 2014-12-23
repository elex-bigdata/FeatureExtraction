package com.elex.ssp.odp;

import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.HiveOperator;
import com.elex.ssp.common.PropertiesUtils;

public class UserTag {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static int gdpUTagMerge() throws SQLException{
		String hql = "INSERT overwrite TABLE odin.gdp_utag_merge select uid,nation,tag,sum(visit) from " +
				" odin.gdp_utag where day >'"+Constants.getStartDay()+"' AND array_contains (array ("+PropertiesUtils.getNations()+"), nation)) " +
				" and tag != '' and tag is not null group by uid,nation,tag";
		System.out.println("==================gdpUTagMerge-sql==================");
		System.out.println(hql);
		System.out.println("==================gdpUTagMerge-sql==================");
		return HiveOperator.executeHQL(hql)?0:1;
	}
	
	public static void tf() throws SQLException{

		String hql = "INSERT OVERWRITE DIRECTORY '"+PropertiesUtils.getRootDir() + Constants.ODPTF+"' select m.uid,m.tag,m.visit,round(m.visit/t.vs,4) from gdp_utag_merge m " +
				"join(select uid,sum(visit) as vs from gdp_utag_merge group by uid)t on m.uid=t.uid)";
		System.out.println("==================UserTag-tf-sql==================");
		System.out.println(hql);
		System.out.println("==================UserTag-tf-sql==================");
		HiveOperator.executeHQL(hql);
	}
	
	public static void idf() throws SQLException{
		String hql = "INSERT OVERWRITE DIRECTORY '"+PropertiesUtils.getRootDir() + Constants.ODPIDF+"'select m.tag,round(log10(t.uc/m.tuc),4) " +
				"from (select tag,count(distinct uid) as tuc from gdp_utag_merge group by tag)m join (select count(distinct uid) as uc from gdp_utag_merge) t on 1=1";
		System.out.println("==================UserTag-idf-sql==================");
		System.out.println(hql);
		System.out.println("==================UserTag-idf-sql==================");
		HiveOperator.executeHQL(hql);
	}
	
	public static void tfidf() throws Exception{
		ToolRunner.run(new Configuration(), new OdpIDF(), null);
	}
	
	public static void loadResult() throws SQLException{
		String hql1 = "load data inpath '"+PropertiesUtils.getRootDir() + Constants.ODPTFIDF+"/part*' overwrite into table "+Constants.TFIDFTABLE +" partition(source='odp')";
		HiveOperator.loadDataToHiveTable(hql1);
		
		String hql2 = "load data inpath '"+PropertiesUtils.getRootDir() + Constants.ODPTFIDF+"/tag*' overwrite into table user_tag";
		HiveOperator.loadDataToHiveTable(hql2);
		
	}

}
