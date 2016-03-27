package fr.emn.integration.gestion_sport.abstraction;


public class Discipline {
	private String nom;
	private String responsable;

	public Discipline(String nom, String responsable) {
		super();
		this.nom = nom;
		this.responsable = responsable;
	}

	public String getNom() {
		return this.nom;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public String getResponsable() {
		return this.responsable;
	}

	public void setResponsable(String responsable) {
		this.responsable = responsable;
	}
	
}
