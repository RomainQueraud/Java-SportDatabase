package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJButtonValidateResultats implements ActionListener {

	private Sport modele;
	private Frame parent;

	public ControlJButtonValidateResultats(Frame parent, Sport modele) {
		this.modele = modele;
		this.parent = parent;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		int choix = JOptionPane.showConfirmDialog(parent,
				"Confirmez-vous la validation ?", "Confirmation",
				JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);
		if (choix == JOptionPane.YES_OPTION)
			this.modele.validateResultats(modele.getDisciplineCourante().getNom(), modele.getCompetitionCourante().getNom());
	}

}
