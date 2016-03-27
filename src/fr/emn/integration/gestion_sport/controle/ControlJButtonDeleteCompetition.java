package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Observable;
import java.util.Observer;

import javax.swing.JButton;
import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJButtonDeleteCompetition implements Observer, ActionListener {

	private Sport modele;
	private JButton button;
	private Frame parent;

	public ControlJButtonDeleteCompetition(Frame parent, Sport modele,
			JButton button) {
		this.modele = modele;
		this.button = button;
		this.parent = parent;
		button.setEnabled(false);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		int choix = JOptionPane.showConfirmDialog(parent,
				"Confirmez-vous la suppression ?", "Confirmation",
				JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE);
		if (choix == JOptionPane.YES_OPTION)
			try {
				this.modele.deleteCompetition(modele.getDisciplineCourante().getNom(), modele.getCompetitionCourante().getNom());
				this.modele.setCompetitionCourante(null);
			} catch (Exception ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parent, "On ne peut pas!",
						"Erreur!", JOptionPane.ERROR_MESSAGE);
			}
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
