package com.elex.ssp.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {

	private static Properties pop = new Properties();
	static {
		InputStream is = null;
		try {
			is = PropertiesUtils.class.getClassLoader().getResourceAsStream(
"conf.properties");
			pop.load(is);
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			try {
				if (is != null)
					is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static String getRootDir() {

		return pop.getProperty("rootdir");
	}

	public static int getMergeDays() {
		return Integer.parseInt(pop.getProperty("mergeDays"));
	}

	public static String getHiveurl() {
		return pop.getProperty("hive.url");
	}

	public static String getHiveUser() {
		return pop.getProperty("hive.user");
	}

	public static String getBeginDay() {

		return pop.getProperty("init.beginday");
	}

	public static String getEndDay() {
		return pop.getProperty("init.endday");
	}

	public static boolean getIsInit() {

		return pop.getProperty("init").equals("true") ? true : false;
	}

	public static String getExpXmlPath() {

		return pop.getProperty("exp");
	}

	public static String getNations() {
		return pop.getProperty("nations");
	}

	public static String getAdid() {
		// TODO Auto-generated method stub
		return pop.getProperty("adid");
	}

}
