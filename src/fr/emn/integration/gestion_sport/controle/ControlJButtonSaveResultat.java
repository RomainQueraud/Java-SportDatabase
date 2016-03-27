package fr.emn.integration.gestion_sport.controle;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Resultat;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.ResultatDialog;

public class ControlJButtonSaveResultat implements ActionListener {

	private Sport modele;
	private ResultatDialog parent;

	public ControlJButtonSaveResultat(ResultatDialog parent, Sport modele) {
		this.modele = modele;
		this.parent = parent;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		try {
			if (parent.getResultat() != -1) {
				Resultat n = new Resultat(parent.getResultat(), parent.getJuge(),
						parent.getDate(), parent.getDefinitive());
				if (modele.findResultat(modele.getDisciplineCourante().getNom(), modele
						.getCompetitionCourante().getNom(), modele
						.getAthleteCourante().getEmail()) == null) {
					modele.saveResultat(n, modele.getDisciplineCourante().getNom(),
							modele.getCompetitionCourante().getNom(), modele
									.getAthleteCourante().getEmail(), true);
					parent.setVisible(false);
				}

				else {

					modele.saveResultat(n, modele.getDisciplineCourante().getNom(),
							modele.getCompetitionCourante().getNom(), modele
									.getAthleteCourante().getEmail(), false);
					parent.setVisible(false);
				}

			} else {
				modele.deleteResultat(modele.getDisciplineCourante().getNom(), modele
						.getCompetitionCourante().getNom(), modele
						.getAthleteCourante().getEmail());
				parent.setVisible(false);
			}
		} catch (Exception ex) {
			ex.printStackTrace();

			JOptionPane.showMessageDialog(parent, "On ne peut pas!", "Erreur!",
					JOptionPane.ERROR_MESSAGE);
		}
	}

}
