package com.elex.ssp.odp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class DmozUtils extends DefaultHandler {

	private String preTag = null;
	private String url = null, host = null, tag, topic;
	private String[] categories;
	private static BufferedWriter out;
	private int i;
	private static Map<String, String> dmoz = new HashMap<String, String>();
	private static int hit=0,lv1=0,wr=0,region=0,region_else=0,all_else=0;

	/**
	 * @param args
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) {
		try {
			loadDmoz();
			out = new BufferedWriter(new FileWriter(args[1]));
			parse(args[0]);
			out.close();
			System.out.println("hit="+hit+";lv1="+lv1+";wr="+wr+";region="+region+";region_else="+region_else+";all_else="+all_else);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void parse(String src) {

		SAXParserFactory factory = SAXParserFactory.newInstance();
		SAXParser parser;

		try {
			parser = factory.newSAXParser();
			DmozUtils handler = new DmozUtils();
			parser.parse(new FileInputStream(new File(src)), handler);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if (qName.equalsIgnoreCase("ExternalPage")) {
			if (attributes != null) {
				try {
					url = attributes.getValue("about");
					if (url != null) {
						if (url.startsWith("http") || url.startsWith("https")) {
							host = new URL(url).getHost();
						} else {
							host = new URL("http://" + url).getHost();
						}
						out.write(host + ",");
					}
				} catch (MalformedURLException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		} else {
			preTag = qName;
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("ExternalPage")) {
			try {
				out.write("\r\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (preTag.equals("topic")) {
			topic = new String(ch, start, length);
			categories = topic.trim().split("/");
			i = categories.length;
			try {
			if (i >= 4) {

				
					if (!categories[1].trim().equals("World") && !categories[1].trim().equals("Regional")) {
						out.write(categories[1].trim() + " ");
						lv1++;
					} else if (categories[1].trim().equals("World")) {
						tag = dmoz.get(categories[3].trim());
						if (tag != null) {
							if(!tag.trim().equals("")){
								out.write(tag + " ");
								hit++;
							}
							
						} else {
							out.write(categories[2].trim() + " ");
							wr++;
						}

					} else if (categories[1].trim().equals("Regional")) {
						if (topic.contains("Arts_and_Entertainment")) {
							out.write("Arts ");
						} else if (topic.contains("Business_and_Economy")|| topic.contains("Real_Estate")) {
							out.write("Business ");
						} else if (topic.contains("Education")) {
							out.write("Education ");
						} else if (topic.contains("Employment")) {
							out.write("Employment ");
						} else if (topic.contains("Government")) {
							out.write("Government ");
						} else if (topic.contains("Guides_and_Directories")) {
							out.write("Reference ");
						} else if (topic.contains("Health")) {
							out.write("Health ");
						} else if (topic.contains("Maps_and_Views")|| topic.contains("Travel_and_Tourism")|| topic.contains("Weather")) {
							out.write("Travel ");
						} else if (topic.contains("News_and_Media")) {
							out.write("News ");
						} else if (topic.contains("Recreation_and_Sports")) {
							out.write("Recreation ");
						} else if (topic.contains("Science_and_Environment")) {
							out.write("Science ");
						} else if (topic.contains("Shopping")) {
							out.write("Shopping ");
						} else if (topic.contains("Society_and_Culture")) {
							out.write("Society ");
						} else if (topic.contains("Transportation")) {
							out.write("Transportation ");
						} else {
							out.write(categories[3] + " ");
							region_else++;
						}
						
						region++;

					}
				

			}else if(i ==3 ){
				if (!categories[1].trim().equals("World") && !categories[1].trim().equals("Regional")) {
					out.write(categories[1].trim() + " ");
					lv1++;
				}else if (categories[1].trim().equals("World")) {
					out.write(categories[2].trim() + " ");
					wr++;
				}
			}else if(i == 2){
				if (!categories[1].trim().equals("World") && !categories[1].trim().equals("Regional")) {
					out.write(categories[1].trim() + " ");
					lv1++;
				}
			}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private static void loadDmoz() throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(Thread
				.currentThread().getContextClassLoader()
				.getResourceAsStream("dmoz.txt")));
		String line;
		String[] kv;
		while ((line = reader.readLine()) != null) {
			kv = line.split(",");
			if (kv.length == 2) {
				dmoz.put(kv[0], kv[1]);
			}

		}
		reader.close();
	}

}
