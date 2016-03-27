package fr.emn.integration.gestion_sport.abstraction;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SportJDBC extends Sport {

	@Override
	public boolean importResultatsFile(File file)
			throws SAXException, IOException,
			ImportException, Exception {
		
		DocumentBuilderFactory factory =
				DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
				
		Document document = (Document) builder.parse(file);
		
		Element resultats = (Element) document.getFirstChild();
		
		NodeList listeResultats =
				resultats.getElementsByTagName("resultat");
		String competition = resultats.getAttribute("competition");
		String discipline = resultats.getAttribute("discipline");
		String juge = resultats.getAttribute("juge");
		String dateResultatString = resultats.getAttribute("dateresultat");
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date dateResultat = new java.sql.Date(formatter.parse(dateResultatString).getTime());
		
		for (int i=0; i<listeResultats.getLength(); i++){
			String athlete = ((Element) listeResultats.item(i)).getAttribute("athlete");
			String valeurString = ((Element) listeResultats.item(i)).getAttribute("valeur");
			float valeur = Float.parseFloat(valeurString);
			
			Resultat resultat = new Resultat(valeur, juge, dateResultat, false);
			this.saveResultat(resultat, discipline, competition, athlete, true);
		}
		return true;
	}

	@Override
	public void validateResultats(String discipline, String competition) {
		Connection conn = BDSport.getConnexion();
		try {
			String sql = "UPDATE Resultat SET (Resultat.definitive = TRUE) WHERE Resultat.discipline = '" + discipline + "' AND Resultat.competition = '" + competition+"'";
			PreparedStatement stmt;
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.setChanged();
		this.notifyObservers(CHANGEMENT_ATHLETES);
	}

	@Override
	public Vector<String> getAllAthletesAndResultatsAsStrings(String discipline, String competition) {
		Vector<String> chaine = new Vector<String>();
		Vector<Athlete> listeAthletes = this.findAllAthletes();
		for (Athlete a : listeAthletes) {
			String listeAthletesResultats = a.getNom()+" "+a.getPrenom()+", "+a.getEmail();
			Resultat resultat = this.findResultat(discipline, competition, a.getEmail());

			if (resultat != null) {
				if (resultat.isDefinitive()) {
					listeAthletesResultats += "; Résultat : " + resultat.getResultat() + ", Juge : " + resultat.getJuge() + ", Date : " + new java.sql.Date(resultat.getDate().getTime()) + ", Etat actuel : définitif.";
				}
				else{
					listeAthletesResultats += "; Résultat : " + resultat.getResultat() + ", Juge : " + resultat.getJuge() + ", Date : " + new java.sql.Date(resultat.getDate().getTime()) + ", Etat actuel : non définitif.";
				}
			}
			chaine.add(listeAthletesResultats);
		}
		return chaine;
	}

	@Override
	public void saveResultat(Resultat n, String discipline, String competition,	String athlete, boolean creation) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql;
		System.out.println("findResultat null ? : " + (this.findResultat(discipline, competition, athlete)==null));
		System.out.println("n null ? : " + n==null);
		System.out.println("creation ? : " + creation);
		if(creation == true && this.findResultat(discipline, competition, athlete) == null){
			sql = String.format("INSERT INTO RESULTAT VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s');", discipline, competition, athlete, ""+n.getResultat(), ""+n.getJuge(), new java.sql.Date(n.getDate().getTime()), ""+n.isDefinitive());
		}
		else {
			sql = String.format("UPDATE resultat SET discipline='%s', competition='%s', athlete='%s', valeur='%s', juge='%s', date='%s', definitive='%s' WHERE discipline='%s'", discipline, competition, athlete, ""+n.getResultat(), ""+n.getJuge(), new java.sql.Date(n.getDate().getTime()), ""+n.isDefinitive(), this.getDisciplineCourante().getNom());
		}
		try {
			PreparedStatement stmt;
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();	
			//TODO : est-il vraiment utile de mettre ces 4 prochaines lignes ?
			this.setChanged();
			this.notifyObservers(CHANGEMENT_ATHLETES);
			this.notifyObservers(CHANGEMENT_DISCIPLINES);
			this.notifyObservers(CHANGEMENT_ATHLETES);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void saveAthlete(Athlete n, Vector<String> disciplines,
			boolean creation) throws Exception {
		Connection conn = BDSport.getConnexion();
		Vector<String> sql = new Vector<String>();
		if(creation){
			sql.add(String.format("INSERT INTO athlete VALUES ('%s','%s','%s');",n.getEmail(), n.getNom(), n.getPrenom()));
			for(int i=0 ; i<disciplines.size() ; i++){
				sql.add(String.format("INSERT INTO disciplineathlete VALUES ('%s','%s');", disciplines.get(i),n.getEmail()));
			}
		}
		else{
			//Pour l'update, on supprime/rajoute les lignes dans disciplineathlete correspondantes
			// Et on peut changer le nom et le prénom dans athlete
			sql.add(String.format("UPDATE athlete SET email='%s', nom='%s', prenom='%s' WHERE email='%s';", n.getEmail(), n.getNom(), n.getPrenom(), this.getAthleteCourante().getEmail()));
			//On supprime toutes les disciplines et on les remet toutes
			sql.add(String.format("DELETE FROM disciplineathlete WHERE athlete='%s';", n.getEmail())); 
			for(int i=0 ; i<disciplines.size() ; i++){
				sql.add(String.format("INSERT INTO DISCIPLINEATHLETE VALUES('%s','%s');", disciplines.get(i), n.getEmail()));
			}
		}
		PreparedStatement stmt;
		try {
			for(int i=0 ; i<sql.size() ; i++){
				stmt = conn.prepareStatement(sql.get(i));
				System.out.println(sql.get(i));
				stmt.executeUpdate();
			}
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_ATHLETES);
			this.notifyObservers(Sport.CHANGEMENT_DISCIPLINES);
		} 
		catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public void saveCompetition(Competition e, boolean creation)
			throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql;
		if(creation){
			sql = String.format("INSERT INTO competition VALUES ('%s','%s','%s','%b');",disciplineCourante.getNom(),e.getNom(),new java.sql.Date(e.getDate().getTime()),e.isOfficielle());
		}
		else{
			System.out.println(e.getDate().toString());
			sql = String.format("UPDATE competition SET date='%s', officielle='%b' WHERE nom='%s' AND discipline='%s';",new java.sql.Date(e.getDate().getTime()),e.isOfficielle(),e.getNom(), disciplineCourante.getNom());
		}
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_COMPETITIONS);
		} 
		catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	@Override
	public void saveDiscipline(Discipline m, boolean creation) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql;
		if(creation){
			sql = String.format("INSERT INTO discipline VALUES ('%s','%s');",m.getNom(),m.getResponsable());
		}
		else{
			sql = String.format("UPDATE discipline SET nom='%s', responsable='%s' WHERE nom='%s' AND responsable='%s';",m.getNom(),m.getResponsable(), m.getNom(), m.getResponsable());
		}
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_DISCIPLINES);
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void deleteResultat(String discipline, String competition,
			String athlete) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql = "DELETE FROM resultat WHERE discipline='"+discipline+"' AND competition='"+competition+"' AND athlete='"+athlete+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers();
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void deleteCompetition(String discipline, String nom) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql = "DELETE FROM competition WHERE discipline='"+discipline+"' AND nom='"+nom+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_COMPETITIONS);
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void deleteAthlete(String email) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql = "DELETE FROM athlete WHERE email='"+email+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_ATHLETES);
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void deleteDiscipline(String m) throws Exception {
		Connection conn = BDSport.getConnexion();
		String sql = "DELETE FROM discipline WHERE Nom='"+m+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			stmt.executeUpdate();
			this.setChanged();
			this.notifyObservers(Sport.CHANGEMENT_DISCIPLINES);
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Resultat findResultat(String discipline, String competition, String athlete) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM RESULTAT WHERE DISCIPLINE='"+discipline+"' AND COMPETITION='"+competition+"' AND ATHLETE='"+athlete+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()){
				Boolean definitive = rs.getBoolean("definitive");
				Date date = rs.getDate("Date");
				String juge = rs.getString("juge");
				Float valeur = rs.getFloat("valeur");
				
				if (definitive == null || date == null || juge == null || valeur == null) {
					return null;
				}
				else {
					return new Resultat(valeur, juge, date, definitive);
				}
			}
			else{
				return null;
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Competition findCompetition(String discipline, String nom) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM competition WHERE Nom='"+nom+"' AND competition.discipline='"+discipline+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			String Nom = "";
			Date Date = null;
			Boolean Officielle = false;
			while (rs.next()) {
				Nom = rs.getString("Nom");
				Date = rs.getDate("Date");
				Officielle = rs.getBoolean("Officielle");
			}
			if(Nom==""){
				return null;
			}
			else{
				Competition c = new Competition(Nom, Date, Officielle);
				return c;
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Vector<Athlete> findAllAthletes() {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM athlete";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			Vector<Athlete> vect = new Vector<Athlete>();
			while (rs.next()) {
				String Nom = rs.getString("Nom");
				String Prenom = rs.getString("Prenom");
				String Email = rs.getString("Email");
				Athlete t = new Athlete(Email, Nom, Prenom);
				vect.add(t);
			}
			return vect;
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Vector<Discipline> findAllDisciplines()  {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM discipline";
		System.out.println();
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
		
			ResultSet rs = stmt.executeQuery();
			
			Vector<Discipline> vect = new Vector<Discipline>();
			
			while (rs.next()) {
				String Nom = rs.getString("Nom");
				String Responsable = rs.getString("Responsable");
				Discipline d = new Discipline(Nom, Responsable);
				vect.add(d);
			}
		return vect;
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Discipline findDiscipline(String nom) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM discipline WHERE Nom='"+nom+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			String Nom = "";
			String Responsable = "";
			while (rs.next()) {
				Nom = rs.getString("Nom");
				Responsable = rs.getString("Responsable");
			}
			if(Nom==""){
				return null;
			}
			else{
				Discipline d = new Discipline(Nom, Responsable);
				return d;
			}
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Vector<Discipline> findDisciplinesAthlete(Athlete athlete) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT DISTINCT * FROM DISCIPLINEATHLETE WHERE ATHLETE='"+athlete.getEmail()+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			Vector<Discipline> vect = new Vector<Discipline>();

			while (rs.next()) {
				String sql2 = "SELECT DISTINCT * FROM DISCIPLINE WHERE NOM='"+rs.getString("discipline")+"'";
				PreparedStatement stmt2;				
				stmt2 = BDSport.getConnexion().prepareStatement(sql2);				
				ResultSet rs2 = stmt2.executeQuery();
				while (rs2.next())
				{
					vect.add(new Discipline(rs2.getString("nom"), rs2.getString("responsable")));
				}
			}
			return vect;
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Vector<Athlete> findAthletesDiscipline(Discipline discipline) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT Nom, Prenom, Email FROM athlete,disciplineathlete WHERE athlete.email=disciplineathlete.athlete AND disciplineathlete.discipline='"+discipline.getNom()+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
			ResultSet rs = stmt.executeQuery();
			Vector<Athlete> vect = new Vector<Athlete>();
			while (rs.next()) {
				String Nom = rs.getString("Nom");
				String Prenom = rs.getString("Prenom");
				String Email = rs.getString("Email");
				Athlete t = new Athlete(Email, Nom, Prenom);
				vect.add(t);
			}
			return vect;
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Vector<Competition> findCompetitionsDiscipline(Discipline discipline) {
		Connection conn = BDSport.getConnexion();
		String sql = "SELECT * FROM competition WHERE discipline='"+discipline.getNom()+"'";
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(sql);
		
			ResultSet rs = stmt.executeQuery();
			
			Vector<Competition> vect = new Vector<Competition>();
			
			while (rs.next()) {
				String Nom = rs.getString("Nom");
				Date Date = rs.getDate("Date");
				Boolean Officielle = rs.getBoolean("Officielle");
				Competition c = new Competition(Nom, Date, Officielle);
				vect.add(c);
			}
		return vect;
		} 
		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void close() {
		// TODO 

	}

}
