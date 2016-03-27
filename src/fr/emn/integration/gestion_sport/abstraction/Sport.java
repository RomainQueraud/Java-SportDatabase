package fr.emn.integration.gestion_sport.abstraction;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Observable;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

public abstract class Sport extends Observable {
	
	protected Discipline disciplineCourante;
	protected Competition competitionCourante;
	protected Athlete athleteCourante;
	
	public static final Integer CHANGEMENT_DISCIPLINE_COURANT = new Integer(1);
	public static final Integer CHANGEMENT_COMPETITION_COURANTE = new Integer(2);
	public static final Integer CHANGEMENT_ATHLETE_COURANT = new Integer(3);
	
	public static final Integer CHANGEMENT_DISCIPLINES = new Integer(4);
	public static final Integer CHANGEMENT_COMPETITIONS = new Integer(5);
	public static final Integer CHANGEMENT_ATHLETES = new Integer(6);

	public Sport() {
		super();
		this.disciplineCourante = null;
		this.competitionCourante = null;
		this.athleteCourante = null;
	}
	
	public Discipline getDisciplineCourante() {
		return disciplineCourante;
	}

	public void setDisciplineCourante(Discipline selectedDiscipline) {
		if (this.disciplineCourante != null
				&& !this.disciplineCourante.equals(selectedDiscipline)
				|| (selectedDiscipline != null && !selectedDiscipline
						.equals(this.disciplineCourante))) {
			this.disciplineCourante = selectedDiscipline;
			this.setCompetitionCourante(null);
			this.setChanged();
			this.notifyObservers(CHANGEMENT_DISCIPLINE_COURANT);
		}
	}

	public Competition getCompetitionCourante() {
		return competitionCourante;
	}

	public void setCompetitionCourante(Competition competitionCourante) {
		if (this.competitionCourante != null
				&& !this.competitionCourante.equals(competitionCourante)
				|| (competitionCourante != null && !competitionCourante
						.equals(this.competitionCourante))) {
			this.competitionCourante = competitionCourante;
			this.setChanged();
			this.notifyObservers(CHANGEMENT_COMPETITION_COURANTE);
		}
	}

	public Athlete getAthleteCourante() {
		return athleteCourante;
	}

	public void setAthleteCourante(Athlete athleteCourante) {
		if (this.athleteCourante != null
				&& !this.athleteCourante.equals(athleteCourante)
				|| (athleteCourante != null && !athleteCourante
						.equals(this.athleteCourante))) {
			this.athleteCourante = athleteCourante;
			this.setChanged();
			this.notifyObservers(CHANGEMENT_ATHLETE_COURANT);
		}
	}

	public abstract boolean importResultatsFile(File file)
			throws ParserConfigurationException, SAXException, IOException,
			ImportException, Exception;

	public abstract void validateResultats(String discipline, String competition);

	public abstract Vector<String> getAllAthletesAndResultatsAsStrings(String discipline,
			String competition);

	public abstract void saveResultat(Resultat n, String discipline, String competition,
			String athlete, boolean creation) throws Exception;

	public abstract void saveAthlete(Athlete n, Vector<String> disciplines, boolean creation)
			throws Exception;

	public abstract void saveCompetition(Competition e, boolean creation)
			throws Exception;

	public abstract void saveDiscipline(Discipline m, boolean creation)
			throws Exception;

	public abstract void deleteResultat(String discipline, String competition, String athlete)
			throws Exception;

	public abstract void deleteCompetition(String discipline, String nom)
			throws Exception;

	public abstract void deleteAthlete(String email) throws Exception;

	public abstract void deleteDiscipline(String m) throws Exception;

	public abstract Resultat findResultat(String discipline, String competition, String athlete);

	public abstract Competition findCompetition(String discipline, String nom);

	public abstract Vector<Athlete> findAllAthletes();

	public abstract Vector<Discipline> findAllDisciplines();

	public abstract Discipline findDiscipline(String nom);
	
	public abstract Vector<Discipline> findDisciplinesAthlete(Athlete athlete);

	public abstract Vector<Athlete> findAthletesDiscipline(Discipline discipline);

	public abstract Vector<Competition> findCompetitionsDiscipline(Discipline discipline);
	
	protected class ImportException extends Exception {

		private static final long serialVersionUID = 1L;
		
		public ImportException(){
			super();
		}
		
	}

	public abstract void close();
	
}

