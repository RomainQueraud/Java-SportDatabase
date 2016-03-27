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

import fr.emn.integration.gestion_sport.abstraction.Competition;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.controle.ControlJButtonSaveCompetition;

public class CompetitionDialog extends JDialog {

	private static final long serialVersionUID = 1L;

	private JTextField nom;
	private JTextField date;
	private JCheckBox officielle;

	public String getNom() {
		return nom.getText();
	}

	public Date getDate() throws ParseException {
			return new SimpleDateFormat("yyyy-MM-dd").parse(date.getText());
	}

	public boolean isOfficielle() {
		return officielle.isSelected();
	}

	public CompetitionDialog(Frame parent, Sport modele, boolean creation) {
		super(parent, "Competition", true);
		this.setMinimumSize(new Dimension(200, 100));

		this.getContentPane().setLayout(new GridLayout(1, 3));

		JPanel form = new JPanel(new GridLayout(6, 1));
		form.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 6));

		form.add(new JLabel("Nom"));
		nom = new JTextField();
		form.add(nom);
		form.add(new JLabel("Date (aaaa-mm-jj)"));
		date = new JTextField();
		form.add(date);
		officielle = new JCheckBox();
		officielle.setText("Officielle");
		form.add(officielle);
		JPanel buttons = new JPanel();
		JButton save = new JButton("OK");
		save.addActionListener(new ControlJButtonSaveCompetition(this, modele,
				creation));
		buttons.add(save);
		form.add(buttons);

		this.getContentPane().add(form);

		if (!creation) {
			Competition courante = modele.findCompetition(
					modele.getDisciplineCourante().getNom(), modele.getCompetitionCourante().getNom());
			nom.setText(courante.getNom());
			date.setText(new SimpleDateFormat("yyyy-MM-dd").format(courante
					.getDate()));
			officielle.setSelected(courante.isOfficielle());
		}

		this.pack();
		super.setLocationRelativeTo(parent);
	}

}
