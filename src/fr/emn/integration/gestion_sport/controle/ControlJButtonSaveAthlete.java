package fr.emn.integration.gestion_sport.controle;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Athlete;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.AthleteDialog;

public class ControlJButtonSaveAthlete implements ActionListener {

	private Sport modele;
	private boolean creation;
	private AthleteDialog parent;

	public ControlJButtonSaveAthlete(AthleteDialog parent, Sport modele,
			boolean creation) {
		this.modele = modele;
		this.parent = parent;
		this.creation = creation;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		Athlete n = new Athlete(parent.getEmail(), parent.getNom(),
				parent.getPrenom());
		try {
			modele.saveAthlete(n, parent.getSelectedDisciplines(), creation);
			modele.setAthleteCourante(n);
			parent.setVisible(false);
		} catch (Exception ex) {
			ex.printStackTrace();
			JOptionPane.showMessageDialog(parent, "On ne peut pas!", "Erreur!",
					JOptionPane.ERROR_MESSAGE);
		}
	}
}
