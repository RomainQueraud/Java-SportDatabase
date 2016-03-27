package fr.emn.integration.gestion_sport.abstraction;

import java.util.Date;

public class Resultat {
	private float valeur;
	private String juge;
	private Date date;
	private boolean definitive;

	public Resultat(float valeur, String juge, Date date, boolean definitive) {
		super();
		this.valeur = valeur;
		this.juge = juge;
		this.date = date;
		this.definitive = definitive;
	}

	public float getResultat() {
		return this.valeur;
	}

	public void setResultat(float valeur) {
		this.valeur = valeur;
	}

	public String getJuge() {
		return this.juge;
	}

	public void setJuge(String juge) {
		this.juge = juge;
	}

	public Date getDate() {
		return this.date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public boolean isDefinitive() {
		return this.definitive;
	}

	public void setDefinitive(boolean definitive) {
		this.definitive = definitive;
	}

}
