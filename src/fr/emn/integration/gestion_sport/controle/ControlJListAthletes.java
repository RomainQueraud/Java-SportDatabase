package fr.emn.integration.gestion_sport.controle;

import java.util.Observable;
import java.util.Observer;
import java.util.Vector;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import fr.emn.integration.gestion_sport.abstraction.Athlete;
import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJListAthletes implements Observer, ListSelectionListener {
	private JList<String> liste;
	private Vector<Athlete> listeAthletes;
	private Sport modele;

	public ControlJListAthletes(Sport modele, JList<String> liste) {
		this.modele = modele;
		this.liste = liste;		
		if (this.modele.getDisciplineCourante()==null) this.listeAthletes = this.modele.findAllAthletes();
		else this.listeAthletes = this.modele.findAthletesDiscipline(this.modele.getDisciplineCourante());
		Vector<String> data=new Vector<String>();
		for (Athlete e : listeAthletes) data.add(e.toString());
		this.liste.setListData(data);
	}

	public void update(Observable o, Object message) {
		Integer iMessage = (Integer) message;
		if (iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT
				|| iMessage == Sport.CHANGEMENT_COMPETITION_COURANTE
				|| iMessage == Sport.CHANGEMENT_ATHLETES) {
			if (this.modele.getCompetitionCourante() == null) {
				if (this.modele.getDisciplineCourante()==null) this.listeAthletes = this.modele.findAllAthletes();
				else this.listeAthletes = this.modele.findAthletesDiscipline(this.modele.getDisciplineCourante());
				Vector<String> data=new Vector<String>();
				for (Athlete e : listeAthletes) data.add(e.toString());
				this.liste.setListData(data);
			} else {
				this.liste.setListData(this.modele.getAllAthletesAndResultatsAsStrings(
						this.modele.getDisciplineCourante().getNom(),
						this.modele.getCompetitionCourante().getNom()));
				if (this.modele.getDisciplineCourante()==null) this.listeAthletes = this.modele.findAllAthletes();
				else this.listeAthletes = this.modele.findAthletesDiscipline(this.modele.getDisciplineCourante());
			}
		}
		if (iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT
				|| iMessage == Sport.CHANGEMENT_COMPETITION_COURANTE
				|| iMessage == Sport.CHANGEMENT_ATHLETES || iMessage == Sport.CHANGEMENT_ATHLETE_COURANT) {
			if (this.modele.getAthleteCourante() == null) 
				this.liste.clearSelection();
			else for (Athlete e: listeAthletes) 
				if (e.getEmail().equals(this.modele.getAthleteCourante().getEmail()))
					this.liste.setSelectedIndex(listeAthletes.indexOf(e));
			
			this.liste.repaint();
		}
	}

	public void valueChanged(ListSelectionEvent e) {
		if (this.liste.getSelectedIndex() != -1
				&& !this.liste.getSelectedValue().equals("-"))
			this.modele.setAthleteCourante(this.listeAthletes.get(this.liste
					.getSelectedIndex()));
		else if (this.liste.getSelectedIndex() != -1)
			this.modele.setAthleteCourante(null);
	}
}