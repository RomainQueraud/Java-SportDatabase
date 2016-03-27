package fr.emn.integration.gestion_sport.presentation;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;

import fr.emn.integration.gestion_sport.abstraction.Athlete;
import fr.emn.integration.gestion_sport.abstraction.Discipline;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.controle.ControlJButtonSaveAthlete;

public class AthleteDialog extends JDialog {

	private static final long serialVersionUID = 1L;

	private JTextField nom;
	private JTextField prenom;
	private JTextField email;

	public String getNom() {
		return nom.getText();
	}

	public String getPrenom() {
		return prenom.getText();
	}

	public String getEmail() {
		return email.getText();
	}

	Vector<Discipline> allDisciplines = new Vector<Discipline>();
	JList<String> listeDisciplines;

	Vector<Discipline> selectedDisciplines = new Vector<Discipline>();
	JList<String> listeSelectedDisciplines;

	public Vector<String> getSelectedDisciplines() {
		Vector<String> data = new Vector<String>();
		for (Discipline m : selectedDisciplines) data.add(m.getNom());
		return data;
	}

	public AthleteDialog(Frame parent, Sport modele, boolean creation) {
		super(parent, "Athlete", true);
		this.getContentPane().setLayout(new GridLayout(1, 3));
		this.setMinimumSize(new Dimension(200, 100));

		JPanel form = new JPanel(new GridLayout(7, 1));
		form.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 3));

		form.add(new JLabel("Nom"));
		nom = new JTextField();
		form.add(nom);
		form.add(new JLabel("Prenom"));
		prenom = new JTextField();
		form.add(prenom);
		form.add(new JLabel("E-mail"));
		email = new JTextField();
		form.add(email);
		JPanel buttons = new JPanel();
		JButton save = new JButton("OK");
		save.addActionListener(new ControlJButtonSaveAthlete(this, modele,
				creation));
		buttons.add(save);
		form.add(buttons);

		this.getContentPane().add(form);

		JPanel disciplinesPanel = new JPanel(new BorderLayout());
		disciplinesPanel.setBorder(BorderFactory.createEmptyBorder(6, 3, 6, 3));
		disciplinesPanel.add(new JLabel("Disciplines"), BorderLayout.NORTH);
		listeDisciplines = new JList<String>();
		listeDisciplines.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		allDisciplines = modele.findAllDisciplines();
		listeDisciplines.setListData(disciplinesAsString(allDisciplines));
		disciplinesPanel.add(new JScrollPane(listeDisciplines));
		JButton addDiscipline = new JButton("->");
		disciplinesPanel.add(addDiscipline, BorderLayout.SOUTH);
		this.getContentPane().add(disciplinesPanel);

		JPanel selectedDisciplinesPanel = new JPanel(new BorderLayout());
		selectedDisciplinesPanel.setBorder(BorderFactory.createEmptyBorder(6, 3, 6,
				6));
		selectedDisciplinesPanel.add(new JLabel("Selected Disciplines"),
				BorderLayout.NORTH);
		listeSelectedDisciplines = new JList<String>();
		listeSelectedDisciplines
				.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		selectedDisciplinesPanel.add(new JScrollPane(listeSelectedDisciplines));
		JButton deleteDiscipline = new JButton("<-");
		selectedDisciplinesPanel.add(deleteDiscipline, BorderLayout.SOUTH);
		this.getContentPane().add(selectedDisciplinesPanel);

		addDiscipline.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				if (!containsDiscipline(selectedDisciplines,
						allDisciplines.get(listeDisciplines.getSelectedIndex())
								.getNom())) {
					selectedDisciplines.add(allDisciplines.get(listeDisciplines
							.getSelectedIndex()));
					listeSelectedDisciplines
							.setListData(disciplinesAsString(selectedDisciplines));
				}
			}
		});

		deleteDiscipline.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				selectedDisciplines.remove(listeSelectedDisciplines.getSelectedIndex());
				listeSelectedDisciplines
						.setListData(disciplinesAsString(selectedDisciplines));
			}
		});

		if (!creation) {
			Athlete courante = modele.getAthleteCourante();
			nom.setText(courante.getNom());
			prenom.setText(courante.getPrenom());
			email.setText(courante.getEmail());
			selectedDisciplines = modele.findDisciplinesAthlete(modele.getAthleteCourante());
			listeSelectedDisciplines.setListData(disciplinesAsString(selectedDisciplines));
		}

		this.pack();
		super.setLocationRelativeTo(parent);
	}

	private static Vector<String> disciplinesAsString(Vector<Discipline> disciplines) {
		Vector<String> result = new Vector<String>();
		for (Discipline m : disciplines) {
			result.add(m.getNom());
		}
		return result;
	}

	private static boolean containsDiscipline(Vector<Discipline> disciplines, String discipline) {
		for (Discipline m : disciplines)
			if (discipline.equals(m.getNom()))
				return true;
		return false;
	}

}
