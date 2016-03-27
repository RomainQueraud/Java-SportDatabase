package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Observable;
import java.util.Observer;

import javax.swing.JButton;

import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.CompetitionDialog;

public class ControlJButtonEditCompetition implements Observer, ActionListener {

	private Sport modele;
	private JButton button;
	Frame parent;

	public ControlJButtonEditCompetition(Frame parent, Sport modele,
			JButton button) {
		this.modele = modele;
		this.button = button;
		this.parent = parent;
		button.setEnabled(false);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		CompetitionDialog competitionDialog = new CompetitionDialog(parent,
				modele, false);
		competitionDialog.setVisible(true);
	}

	@Override
	public void update(Observable o, Object message) {
		Integer iMessage = (Integer) message;
		if (iMessage == Sport.CHANGEMENT_COMPETITION_COURANTE) {
			if (modele.getCompetitionCourante() == null)
				button.setEnabled(false);
			else
				button.setEnabled(true);
		}
	}

}
