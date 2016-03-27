package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Observable;
import java.util.Observer;

import javax.swing.JButton;

import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.DisciplineDialog;

public class ControlJButtonEditDiscipline implements Observer, ActionListener {

	private Sport modele;
	private JButton button;
	Frame parent;

	public ControlJButtonEditDiscipline(Frame parent, Sport modele, JButton button) {
		this.modele = modele;
		this.button = button;
		this.parent = parent;
		button.setEnabled(false);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		DisciplineDialog disciplineDialog = new DisciplineDialog(parent, modele, false);
		disciplineDialog.setVisible(true);
	}

	@Override
	public void update(Observable o, Object message) {
		Integer iMessage = (Integer) message;
		if (iMessage == Sport.CHANGEMENT_DISCIPLINE_COURANT) {
			if (modele.getDisciplineCourante() == null)
				button.setEnabled(false);
			else
				button.setEnabled(true);
		}
	}
}
