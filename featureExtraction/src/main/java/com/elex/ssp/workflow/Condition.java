package com.elex.ssp.workflow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.dom4j.Element;


public class Condition {
	
	private Map<String,Boolean> all;
	private StringBuilder sb;
	private XmlReader xr;
	private StringBuffer itemSB;
	public Condition() {
		all = new HashMap<String,Boolean>();
		sb = new StringBuilder(200);
		xr = XmlReader.getXmlReaderInstance();
		itemSB = new StringBuffer(100);
	}
	
	private void buildAllConditionSent(Element allElement){		
		for (Iterator i = allElement.elementIterator(); i.hasNext();) {
            Element node = (Element) i.next();
            if(!node.getText().equals("all")){
            	all.put(node.getText(), true);
            	sb.append("and ");
            	if(node.attributeValue("type").equals("string")){           		
            		sb.append(getStringNodeCondition(node));
            	}else if(node.attributeValue("type").equals("int") || node.attributeValue("type").equals("double")){
            		sb.append(getNumberNodeCondition(node));
            	}else if(node.attributeValue("type").equals("string_int")){
            		sb.append(getStrinIntNodeCondition(node));
            	}
            } 
        }
		
		
	}
	
	private void BuildItemConditionSent(Element ItemElement){
		int times = 0;
		boolean flag = false;
		StringBuilder featureSB = new StringBuilder(100);
		featureSB.append("when '"+ItemElement.getName()+"' then ");
		for (Iterator i = ItemElement.elementIterator(); i.hasNext();) {
            Element node = (Element) i.next();            
            if(!node.getText().equals("all") && all.get(node.getName())==null){    
            	flag = true;
            	times++;
            	if(times>1){
            		featureSB.append("and ");
            	}
            	if(node.attributeValue("type").equals("string")){           		
            		featureSB.append(getStringNodeCondition(node));
            	}else if(node.attributeValue("type").equals("int") || node.attributeValue("type").equals("double")){
            		featureSB.append(getNumberNodeCondition(node));
            	}else if(node.attributeValue("type").equals("string_int")){
            		featureSB.append(getStrinIntNodeCondition(node));
            	}
            } 
        }
		if(flag){
			itemSB.append(featureSB.toString());
		}
	}

	private String getStrinIntNodeCondition(Element node) {
		String name = node.getName();
		String value = node.getText();
		String key = node.attributeValue("key")==null?name:node.attributeValue("key");
		
		if(value.contains("(")){
			return "array_contains(array("+node.getText().replaceAll("[(|)]", "")+"),"+key+") ";
		}else if(value.matches("[\\d]+\\,[\\d]+")){
			String[] values = value.split(",");
			return "cast("+key+" as int)" +">="+values[0]+" and cast("+key+" as int)" +"<"+values[1]+" ";
		}else{
			return "1=1 ";
		}
	}


	private String getNumberNodeCondition(Element node) {

		String name = node.getName();
		String value = node.getText();
		String key = node.attributeValue("key")==null?name:node.attributeValue("key");
		String type = node.attributeValue("type");
		
		String result = "1=1 ";
		if(type.equals("int")){
			if(value.matches("[\\d]+\\,[\\d]+")){
				String[] values = value.split(",");
				return key+">="+values[0]+" and "+key+" <"+values[1]+" ";
			}else{
				return result;
			}
		}else if(type.equals("double")){
			if(value.matches("[\\d]+\\.[\\d]+\\,[\\d]+\\.[\\d]+")){
				String[] values = value.split(",");
				return key+">="+values[0]+" and "+key+" <"+values[1]+" ";
			}else{
				return result;
			}
		}else{
			return result;
		}
	}


	private String getStringNodeCondition(Element node) {
		
		String name = node.getName();
		String value = node.getText();
		String key = node.attributeValue("key")==null?name:node.attributeValue("key");
		
		if(value.contains("(")){
			return "array_contains(array("+node.getText().replaceAll("[(|)]", "")+"),"+key+") ";
		}else{
			return "1=1 ";
		}
	}
	
	public String createExportConditionSent(String merge){
		String[] items = xr.getFeatureExceptAllList(merge);
		Element allElement = xr.getRoot().element(merge).element("all");
		if(allElement != null){
			buildAllConditionSent(allElement);
		}
		
		for(String item:items){
			Element ItemElement = xr.getRoot().element(merge).element(item);
			BuildItemConditionSent(ItemElement);
		}
		if(!itemSB.toString().equals("")){
			sb.append(" and case ft ");
			sb.append(itemSB);
			sb.append(" else 1=1 ");
			sb.append(" end ");
		}
		return sb.toString();
	}
	
	
}
