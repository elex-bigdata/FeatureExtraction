package com.elex.ssp.feature.tfidf;

import java.io.Serializable;

public class KeyWord implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1946835306508872307L;

	private String word;
	private int wc;
	private double tf;
	private double idf;
	private double tfidf;
	
	
	public KeyWord(String word, int wc, double tf, double idf, double tfidf) {
		super();
		this.word = word;
		this.wc = wc;
		this.tf = tf;
		this.idf = idf;
		this.tfidf = tfidf;
	}
	
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public int getWc() {
		return wc;
	}
	public void setWc(int wc) {
		this.wc = wc;
	}
	public double getTf() {
		return tf;
	}
	public void setTf(double tf) {
		this.tf = tf;
	}
	public double getIdf() {
		return idf;
	}
	public void setIdf(double idf) {
		this.idf = idf;
	}
	public double getTfidf() {
		return tfidf;
	}
	public void setTfidf(double tfidf) {
		this.tfidf = tfidf;
	}
	
}
