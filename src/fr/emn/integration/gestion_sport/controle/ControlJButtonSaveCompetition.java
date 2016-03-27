package fr.emn.integration.gestion_sport.controle;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Competition;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.CompetitionDialog;

public class ControlJButtonSaveCompetition implements ActionListener {

	private Sport modele;
	private boolean creation;
	private CompetitionDialog parent;

	public ControlJButtonSaveCompetition(CompetitionDialog parent, Sport modele,
			boolean creation) {
		this.modele = modele;
		this.parent = parent;
		this.creation = creation;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		try {
			Competition n = new Competition(parent.getNom(), parent.getDate(),
					parent.isOfficielle());
			modele.saveCompetition(n, creation);
			modele.setCompetitionCourante(n);
			parent.setVisible(false);
		} catch (Exception ex) {
			ex.printStackTrace();

			JOptionPane.showMessageDialog(parent, "On ne peut pas!", "Erreur!",
					JOptionPane.ERROR_MESSAGE);
		}
	}

}
