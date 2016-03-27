package fr.emn.integration.gestion_sport.presentation;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import fr.emn.integration.gestion_sport.abstraction.Discipline;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.controle.ControlJButtonSaveDiscipline;

public class DisciplineDialog extends JDialog {

	private static final long serialVersionUID = 1L;

	private JTextField nom;
	private JTextField responsable;

	public String getNom() {
		return this.nom.getText();
	}

	public String getResponsable() {
		return this.responsable.getText();
	}

	public DisciplineDialog(Frame parent, Sport modele, boolean creation) {
		super(parent, "Discipline", true);
	//	this.getContentPane().setLayout(new GridLayout(1, 3));
		this.setMinimumSize(new Dimension(200, 100));

		JPanel form = new JPanel(new GridLayout(5, 1));
		form.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 6));

		form.add(new JLabel("Nom"));
		this.nom = new JTextField();
		form.add(nom);
		form.add(new JLabel("Responsable"));
		this.responsable = new JTextField();
		form.add(responsable);
		JPanel buttons = new JPanel();
		JButton save = new JButton("OK");
		save.addActionListener(new ControlJButtonSaveDiscipline(this, modele,
				creation));
		buttons.add(save);
		form.add(buttons);
		this.getContentPane().add(form);

		if (!creation) {
			Discipline courante = modele.getDisciplineCourante();
			this.nom.setText(courante.getNom());
			this.responsable.setText(courante.getResponsable());
		}

		this.pack();
		super.setLocationRelativeTo(parent);
	}

}
