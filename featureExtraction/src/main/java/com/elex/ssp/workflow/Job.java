package com.elex.ssp.workflow;

import java.sql.SQLException;
import java.text.ParseException;

import com.elex.ssp.common.Constants;
import com.elex.ssp.common.PropertiesUtils;

public abstract class Job {

	public int process(boolean isInit) throws SQLException, ParseException {

		String yestoday = Constants.getYestoday();
		int result = 0;
		if (isInit) {
			String begin = PropertiesUtils.getBeginDay();
			String end = PropertiesUtils.getEndDay();
			String[] days = Constants.getBetweenDate(begin, end);
			for (String day : days) {
				result = doJob(day);
			}

		} else {
			result = doJob(yestoday);
		}

		return result;
	}

	public abstract int doJob(String yestoday) throws SQLException;

}
