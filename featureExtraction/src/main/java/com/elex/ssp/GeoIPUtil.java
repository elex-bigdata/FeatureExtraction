package com.elex.ssp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

;

public class GeoIPUtil {

	/**
	 * @param args
	 * @throws IOException
	 * @throws GeoIp2Exception
	 */
	public static void main(String[] args) throws IOException {
		process(new File(args[0]), args[1]);

	}

	private static void process(File file, String dist) throws IOException {

		BufferedReader in;

		BufferedWriter out = new BufferedWriter(new FileWriter(dist));

		String[] kv;

		File database = new File("D:\\geoip\\GeoIP2-City.mmdb");

		DatabaseReader reader = new DatabaseReader.Builder(database).build();

		InetAddress ipAddress;

		CityResponse response;

		Country country;
		
		City city;
		
		String ip;

		in = new BufferedReader(new FileReader(file));
		String line = in.readLine();
		out.write("COUNTRY,"+  "CITY," +"IMPR,"+"CLICK,"+ "NAME/IP\r\n");
		while (line != null) {
			kv = line.trim().split(",");
			if (kv.length == 4) {
				ip = kv[0]+"."+kv[1];
				try {
					
					ipAddress = InetAddress.getByName(ip+ ".10.10");
					response = reader.city(ipAddress);
					city = response.getCity();
					country = response.getCountry();
					if(city.getName() != null && country.getIsoCode() != null){
						out.write(country.getIsoCode()+","+city.getName() + "," + kv[2] + "," + kv[3] +","+"name"+ "\r\n");
					}else{
						out.write("BR,"+ip + "," + kv[2] + "," + kv[3] +","+"ip"+ "\r\n");
					}
					
				} catch (GeoIp2Exception e) {
					out.write("BR"+ip + "," + kv[2] + "," + kv[3] +","+"ip"+ "\r\n");
				}
				
			}

			line = in.readLine();
		}
		in.close();

		out.close();

	}

}
