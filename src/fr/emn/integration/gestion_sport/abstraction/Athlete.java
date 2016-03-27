package fr.emn.integration.gestion_sport.abstraction;


public class Athlete {
	private String email;
	private String nom;
	private String prenom;

	public Athlete(String email, String nom, String prenom) {
		super();
		this.email = email;
		this.nom = nom;
		this.prenom = prenom;
	}

	public String getEmail() {
		return this.email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getNom() {
		return this.nom;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public String getPrenom() {
		return this.prenom;
	}

	public void setPrenom(String prenom) {
		this.prenom = prenom;
	}
	
	public String toString() {
		return this.getNom() + ", " + this.getPrenom() + ", " + this.getEmail();
	}

}
