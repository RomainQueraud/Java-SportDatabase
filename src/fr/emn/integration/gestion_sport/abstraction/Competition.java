package fr.emn.integration.gestion_sport.abstraction;

import java.util.Date;

public class Competition {
	private String nom;
	private Date date;
	private boolean officielle;

	public Competition(String nom, Date date, boolean officielle) {
		super();
		this.nom = nom;
		this.date = date;
		this.officielle = officielle;
	}

	public String getNom() {
		return this.nom;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public Date getDate() {
		return this.date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public boolean isOfficielle() {
		return this.officielle;
	}

	public void setOfficielle(boolean officielle) {
		this.officielle = officielle;
	}
}
