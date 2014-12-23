package com.elex.ssp.workflow;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.elex.ssp.common.PropertiesUtils;


public class XmlReader {
	
	private  Document document;
	
	private  Element root;
	
	private static XmlReader xr;
	
	private XmlReader() {
		String file = PropertiesUtils.getExpXmlPath();
		File inputXml = new File(file);
        SAXReader saxReader = new SAXReader();
        try {
			document = saxReader.read(inputXml);
			root = document.getRootElement();
		} catch (DocumentException e) {
			e.printStackTrace();
		}       
	}
	
	public static XmlReader getXmlReaderInstance(){
		if(xr==null){
			xr = new XmlReader();
		}
		return xr;
	}
	
	public Document getDocument() {
		return XmlReader.getXmlReaderInstance().document;
	}

	public Element getRoot() {
		return XmlReader.getXmlReaderInstance().root;
	}
	
	public String getAttributeValue(String merge,String feature,String attr){
		return XmlReader.getXmlReaderInstance().getRoot().element(merge)==null?
				null:XmlReader.getXmlReaderInstance().getRoot().element(merge).element(feature)==null?
						null:XmlReader.getXmlReaderInstance().getRoot().element(merge).element(feature).element(attr)==null?
								null:XmlReader.getXmlReaderInstance().getRoot().element(merge).element(feature).element(attr).getText();
	}
	
	public String[] getFeatureExceptAllList(String merge){
		List<String> features = new ArrayList<String>();
		Element e =XmlReader.getXmlReaderInstance().getRoot().element(merge);
		if(e!=null){
			for (Iterator i = e.elementIterator(); i.hasNext();) {
	            Element node = (Element) i.next();
	            if(!node.getName().equals("all")){
	            	features.add(node.getName());
	            }            	           
	        }
		}
		
		return features.toArray(new String[features.size()]);
	}
	
	public String[] getAttrbutes(String merge,String feature){
		
		List<String> features = new ArrayList<String>();
		Element e =XmlReader.getXmlReaderInstance().getRoot().element(merge);
		if(e!=null){
			Element node = (Element) e.element(feature);
			if(node != null){
				for (Iterator k = node.elementIterator(); k.hasNext();) {
	            	Element attr = (Element) k.next();
	            	features.add(attr.getName()+":"+attr.getText());
	            }
			}
			
		}
		
		return features.toArray(new String[features.size()]);
	}
	
	


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		XmlReader xr = XmlReader.getXmlReaderInstance();
	
		
		//xr.parserXml();
		
		//System.out.println(xr.getAttributeValue("featureMerge", "query_length", "fv"));
		System.out.println("15.23,6.85".matches("[\\d]+\\.[\\d]+\\,[\\d]+\\.[\\d]+"));
		
		/*for(String f:xr.getAttrbutes("featureMerge","user")){
			System.out.println(f);
		}*/
		
	}
	
	public void parserXml() {

        for (Iterator i = root.elementIterator(); i.hasNext();) {
            Element conf = (Element) i.next();
            
            for (Iterator j = conf.elementIterator(); j.hasNext();) {
                Element ft = (Element) j.next();
                
                for (Iterator k = ft.elementIterator(); k.hasNext();) {
                    Element node = (Element) k.next();
                    
                    System.out.println(node.getName() + ":" + node.getText());
                }
            }
           
        }
    }
	
	
	


}
