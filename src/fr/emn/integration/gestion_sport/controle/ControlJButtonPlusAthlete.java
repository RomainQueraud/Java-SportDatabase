package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.AthleteDialog;

public class ControlJButtonPlusAthlete implements ActionListener {

	private Sport modele;
	private Frame parent;

	public ControlJButtonPlusAthlete(Frame parent, Sport modele) {
		this.modele = modele;
		this.parent = parent;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		AthleteDialog athleteDialog = new AthleteDialog(parent, modele, true);
		athleteDialog.setVisible(true);
	}

}
