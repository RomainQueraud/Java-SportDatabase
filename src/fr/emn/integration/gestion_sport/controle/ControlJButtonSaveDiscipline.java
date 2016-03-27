package fr.emn.integration.gestion_sport.controle;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JOptionPane;

import fr.emn.integration.gestion_sport.abstraction.Discipline;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.presentation.DisciplineDialog;

public class ControlJButtonSaveDiscipline implements ActionListener {

	private Sport modele;
	private boolean creation;
	private DisciplineDialog parent;

	public ControlJButtonSaveDiscipline(DisciplineDialog parent, Sport modele,
			boolean creation) {
		this.modele = modele;
		this.creation = creation;
		this.parent = parent;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		Discipline m = new Discipline(parent.getNom(), parent.getResponsable());
		try {
			modele.saveDiscipline(m, creation);
			modele.setDisciplineCourante(m);
			parent.setVisible(false);
		} catch (Exception ex) {
			ex.printStackTrace();

			JOptionPane.showMessageDialog(parent, "On ne peut pas!", "Erreur!",
					JOptionPane.ERROR_MESSAGE);
		}
	}

}
