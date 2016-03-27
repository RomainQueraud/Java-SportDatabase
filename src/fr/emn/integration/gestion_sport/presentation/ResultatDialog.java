package fr.emn.integration.gestion_sport.presentation;

import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import fr.emn.integration.gestion_sport.abstraction.Athlete;
import fr.emn.integration.gestion_sport.abstraction.Resultat;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.controle.ControlJButtonSaveResultat;

public class ResultatDialog extends JDialog {

	private static final long serialVersionUID = 1L;

	private JTextField valeur;
	private JTextField juge;
	private JTextField date;
	private JCheckBox definitive;

	public float getResultat() {
		if (!valeur.getText().equals(""))
			return Float.parseFloat(valeur.getText());
		else
			return -1;
	}

	public String getJuge() {
		return juge.getText();
	}

	public Date getDate() throws ParseException {
		if (!date.getText().equals(""))
			return new SimpleDateFormat("yyyy-MM-dd").parse(date.getText());
		else
			return new Date();
	}

	public boolean getDefinitive() {
		return definitive.isSelected();
	}

	public ResultatDialog(Frame parent, Sport modele, boolean creation) {
		super(parent, "Élève", true);
		this.getContentPane().setLayout(new GridLayout(1, 3));
		this.setMinimumSize(new Dimension(200, 100));

		JPanel form = new JPanel(new GridLayout(7, 1));
		form.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 3));

		form.add(new JLabel("Nom"));
		JTextField nom = new JTextField();
		form.add(nom);
		form.add(new JLabel("Prenom"));
		JTextField prenom = new JTextField();
		form.add(prenom);
		form.add(new JLabel("E-mail"));
		JTextField email = new JTextField();
		form.add(email);
		JPanel buttons = new JPanel();
		JButton save = new JButton("OK");
		save.addActionListener(new ControlJButtonSaveResultat(this, modele));
		buttons.add(save);
		form.add(buttons);

		this.getContentPane().add(form);

		Athlete courante = modele.getAthleteCourante();
		nom.setText(courante.getNom());
		prenom.setText(courante.getPrenom());
		email.setText(courante.getEmail());

		nom.setEnabled(false);
		prenom.setEnabled(false);
		email.setEnabled(false);

		JPanel valeurPanel = new JPanel(new GridLayout(7, 1));
		valeurPanel.setBorder(BorderFactory.createEmptyBorder(6, 3, 6, 6));

		valeurPanel.add(new JLabel("Resultat"));
		valeur = new JTextField();
		valeurPanel.add(valeur);
		valeurPanel.add(new JLabel("Juge"));
		juge = new JTextField();
		valeurPanel.add(juge);
		valeurPanel.add(new JLabel("Date (aaaa-mm-jj)"));
		date = new JTextField();
		valeurPanel.add(date);
		definitive = new JCheckBox();
		definitive.setText("Definitive");
		valeurPanel.add(definitive);

		Resultat n = modele.findResultat(modele.getDisciplineCourante().getNom(),
				modele.getCompetitionCourante().getNom(), modele.getAthleteCourante().getEmail());

		if (n != null) {
			valeur.setText("" + n.getResultat());
			juge.setText(n.getJuge());
			date.setText(new SimpleDateFormat("yyyy-MM-dd").format(n.getDate()));
			definitive.setSelected(n.isDefinitive());

			if (n.isDefinitive()) {
				valeur.setEnabled(false);
			}
		}

		this.getContentPane().add(valeurPanel);

		this.pack();
		super.setLocationRelativeTo(parent);
	}

}
