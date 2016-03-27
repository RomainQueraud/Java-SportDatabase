package fr.emn.integration.gestion_sport.controle;

import java.util.Observable;
import java.util.Observer;
import java.util.Vector;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import fr.emn.integration.gestion_sport.abstraction.Discipline;
import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJListDisciplines implements Observer, ListSelectionListener {
	private JList<String> liste;
	private Sport modele;

	public ControlJListDisciplines(Sport modele, JList<String> liste) {
		this.modele = modele;
		this.liste = liste;
		Vector<String> data = new Vector<String>();
		for (Discipline i : this.modele.findAllDisciplines()) data.add(i.getNom());
		data.add(0, "-");
		this.liste.setListData(data);
		this.liste.setSelectedIndex(0);
	}

	public void update(Observable o, Object message) {
		Integer iMessage = (Integer) message;
		if (iMessage == Sport.CHANGEMENT_DISCIPLINES) {
			Vector<String> data = new Vector<String>();
			for (Discipline i : this.modele.findAllDisciplines()) data.add(i.getNom());
			data.add(0, "-");
			this.liste.setListData(data);
		}
		if (iMessage == Sport.CHANGEMENT_DISCIPLINES || iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT) {
			if (this.modele.getDisciplineCourante()!= null)
				this.liste.setSelectedValue(this.modele.getDisciplineCourante().getNom(), true);
			else
				this.liste.setSelectedIndex(0);
			this.liste.repaint();
		}
	}

	public void valueChanged(ListSelectionEvent e) {
		if (this.liste.getSelectedIndex() != -1
				&& !this.liste.getSelectedValue().equals("-"))
			this.modele.setDisciplineCourante(this.modele.findDiscipline(this.liste.getSelectedValue()));
		else if (this.liste.getSelectedIndex() != -1)
			this.modele.setDisciplineCourante(null);
	}
}