package fr.emn.integration.gestion_sport.controle;

import java.util.Observable;
import java.util.Observer;
import java.util.Vector;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import fr.emn.integration.gestion_sport.abstraction.Competition;
import fr.emn.integration.gestion_sport.abstraction.Discipline;
import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJListCompetitions implements Observer, ListSelectionListener {
	private JList<String> liste;
	private Sport modele;

	public ControlJListCompetitions(Sport modele, JList<String> liste) {
		this.modele = modele;
		this.liste = liste;
		String[] data = { "-" };
		this.liste.setListData(data);
		this.liste.setSelectedIndex(0);
	}

	public void update(Observable o, Object message) {
		Integer iMessage = (Integer) message;
		if (iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT
				|| iMessage == Sport.CHANGEMENT_COMPETITIONS) {
			Discipline disciplineCourante = this.modele.getDisciplineCourante();
			if (disciplineCourante != null) {
				Vector<String> data = new Vector<String>();
				for (Competition e : this.modele.findCompetitionsDiscipline(this.modele.getDisciplineCourante())) 
					data.add(e.getNom());
				data.add(0, "-");
				this.liste.setListData(data);
			} else {
				String[] data = { "-" };
				this.liste.setListData(data);
			}
		}
		if (iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT
				|| iMessage == Sport.CHANGEMENT_COMPETITIONS || 
					iMessage == Sport.CHANGEMENT_COMPETITION_COURANTE) {
			if (this.modele.getCompetitionCourante() != null) this.liste.setSelectedValue(this.modele.getCompetitionCourante().getNom(), true);
			else this.liste.setSelectedIndex(0);
			this.liste.repaint();
		}
	}

	public void valueChanged(ListSelectionEvent e) {
		if (this.liste.getSelectedIndex() != -1
				&& !this.liste.getSelectedValue().equals("-"))
			this.modele.setCompetitionCourante(modele.findCompetition(this.modele.getDisciplineCourante().getNom(), this.liste.getSelectedValue()));
		else if (this.liste.getSelectedIndex() != -1)
			this.modele.setCompetitionCourante(null);
	}
}