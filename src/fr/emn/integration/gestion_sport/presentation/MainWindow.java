package fr.emn.integration.gestion_sport.presentation;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.controle.ControlJButtonDeleteAthlete;
import fr.emn.integration.gestion_sport.controle.ControlJButtonDeleteCompetition;
import fr.emn.integration.gestion_sport.controle.ControlJButtonDeleteDiscipline;
import fr.emn.integration.gestion_sport.controle.ControlJButtonEditAthlete;
import fr.emn.integration.gestion_sport.controle.ControlJButtonEditCompetition;
import fr.emn.integration.gestion_sport.controle.ControlJButtonEditDiscipline;
import fr.emn.integration.gestion_sport.controle.ControlJButtonEditResultat;
import fr.emn.integration.gestion_sport.controle.ControlJButtonImportResultats;
import fr.emn.integration.gestion_sport.controle.ControlJButtonPlusAthlete;
import fr.emn.integration.gestion_sport.controle.ControlJButtonPlusCompetition;
import fr.emn.integration.gestion_sport.controle.ControlJButtonPlusDiscipline;
import fr.emn.integration.gestion_sport.controle.ControlJButtonValidateResultats;
import fr.emn.integration.gestion_sport.controle.ControlJFrameMainWindow;
import fr.emn.integration.gestion_sport.controle.ControlJListAthletes;
import fr.emn.integration.gestion_sport.controle.ControlJListCompetitions;
import fr.emn.integration.gestion_sport.controle.ControlJListDisciplines;

public class MainWindow extends JFrame {

	private static final long serialVersionUID = 1L;
	public static final int LABEL_SIZE = 18;
	
	private Sport modele;

	public MainWindow(Sport sport) {
		super("Gestion Sport");
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		this.addWindowListener(new ControlJFrameMainWindow(sport));
		
		this.modele = sport;

		this.getContentPane().setLayout(new BorderLayout());

		this.creerHaut();
		this.creerCentre();
		this.pack();
	}
	
	private void creerHaut() {
		JPanel panneauHaut = new JPanel(new GridLayout(1, 3));

		panneauHaut.setPreferredSize(new Dimension(800, 200));

	// === DISCIPLINES ===
		JPanel panelDisciplines = new JPanel(new BorderLayout());
		panelDisciplines.setBorder(BorderFactory.createEmptyBorder(6, 6, 3, 3));
        // - titre (nord)
		JLabel labelDiscipline = new JLabel("Discipline");
		labelDiscipline.setFont(new Font(labelDiscipline.getFont().getName(), Font.BOLD, LABEL_SIZE));
		panelDisciplines.add(labelDiscipline, BorderLayout.NORTH);
		// - JList disciplines (centre)
		JList<String> listeDisciplines = new JList<String>();
		listeDisciplines.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

		ControlJListDisciplines controleListeDisciplines = new ControlJListDisciplines(
				this.modele, listeDisciplines);
		listeDisciplines.addListSelectionListener(controleListeDisciplines);
		this.modele.addObserver(controleListeDisciplines);
		panelDisciplines.add(new JScrollPane(listeDisciplines), BorderLayout.CENTER);
		// - boutons (sud)
		JPanel panelButtonsDisciplines = new JPanel();// FlowLayout par defaut
		JButton plusDiscipline = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("add.gif")));
		plusDiscipline.setToolTipText("ajouter une nouvelle discipline");
		ControlJButtonPlusDiscipline controlPlusDiscipline = new ControlJButtonPlusDiscipline(
				this, this.modele);
		plusDiscipline.addActionListener(controlPlusDiscipline);
		panelButtonsDisciplines.add(plusDiscipline);
		JButton moinsDiscipline = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("delete.gif")));
		moinsDiscipline.setToolTipText("supprimer la discipline");
		ControlJButtonDeleteDiscipline controlDeleteDiscipline = new ControlJButtonDeleteDiscipline(
				this, this.modele, moinsDiscipline);
		moinsDiscipline.addActionListener(controlDeleteDiscipline);
		this.modele.addObserver(controlDeleteDiscipline);
		panelButtonsDisciplines.add(moinsDiscipline);
		JButton editDiscipline = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("edit.gif")));
		editDiscipline.setToolTipText("modifier la discipline");
		ControlJButtonEditDiscipline controlEditDiscipline = new ControlJButtonEditDiscipline(
				this, this.modele, editDiscipline);
		editDiscipline.addActionListener(controlEditDiscipline);
		this.modele.addObserver(controlEditDiscipline);
		panelButtonsDisciplines.add(editDiscipline);
		panelDisciplines.add(panelButtonsDisciplines, BorderLayout.SOUTH);
		panneauHaut.add(panelDisciplines);

	// === COMPETITIONS ===
		JPanel panelCompetitions = new JPanel(new BorderLayout());
		panelCompetitions.setBorder(BorderFactory.createEmptyBorder(6, 3, 3, 6));
        // - titre (nord)
		JLabel labelCompetition = new JLabel("Competition");
		labelCompetition.setFont(new Font(labelCompetition.getFont().getName(), Font.BOLD, LABEL_SIZE));
		panelCompetitions.add(labelCompetition, BorderLayout.NORTH);

		// - JList competitions (centre)
		JList<String> listeCompetitions = new JList<String>();
		listeCompetitions.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

		ControlJListCompetitions controleListeCompetitions = new ControlJListCompetitions(
				this.modele, listeCompetitions);
		listeCompetitions.addListSelectionListener(controleListeCompetitions);
		this.modele.addObserver(controleListeCompetitions);
		panelCompetitions.add(new JScrollPane(listeCompetitions), BorderLayout.CENTER);

		// - boutons (sud)
		JPanel panelButtonsCompetitions = new JPanel();
		JButton plusCompetition = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("add.gif")));
		plusCompetition.setToolTipText("ajouter une competition");
		ControlJButtonPlusCompetition controlPlusCompetition = new ControlJButtonPlusCompetition(
				this, this.modele, plusCompetition);
		plusCompetition.addActionListener(controlPlusCompetition);
		this.modele.addObserver(controlPlusCompetition);
		panelButtonsCompetitions.add(plusCompetition);
		JButton moinsCompetition = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("delete.gif")));
		moinsCompetition.setToolTipText("supprimer la competition");
		ControlJButtonDeleteCompetition controlDeleteCompetition = new ControlJButtonDeleteCompetition(
				this, this.modele, moinsCompetition);
		moinsCompetition.addActionListener(controlDeleteCompetition);
		this.modele.addObserver(controlDeleteCompetition);
		panelButtonsCompetitions.add(moinsCompetition);
		JButton editCompetition = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("edit.gif")));
		editCompetition.setToolTipText("modifier la competition");
		ControlJButtonEditCompetition controlEditCompetition = new ControlJButtonEditCompetition(
				this, this.modele, editCompetition);
		editCompetition.addActionListener(controlEditCompetition);
		this.modele.addObserver(controlEditCompetition);
		panelButtonsCompetitions.add(editCompetition);
		panelCompetitions.add(panelButtonsCompetitions, BorderLayout.SOUTH);

		panneauHaut.add(panelCompetitions);

		this.getContentPane().add(panneauHaut, BorderLayout.NORTH);
	}

	private void creerCentre() {
		JPanel panelAthletes = new JPanel(new BorderLayout());
		panelAthletes.setPreferredSize(new Dimension(400, 400));
		panelAthletes.setBorder(BorderFactory.createEmptyBorder(3, 6, 6, 6));

	    // - titre (nord)
		JLabel labelAthlete = new JLabel("Athlete");
		labelAthlete.setFont(new Font(labelAthlete.getFont().getName(), Font.BOLD, LABEL_SIZE));
		panelAthletes.add(labelAthlete, BorderLayout.NORTH);
		
		// - JList athletes (centre)
		JList<String> listeAthletes = new JList<String>();
		listeAthletes.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		ControlJListAthletes controleListeAthletes = new ControlJListAthletes(
				this.modele, listeAthletes);
		listeAthletes.addListSelectionListener(controleListeAthletes);
		this.modele.addObserver(controleListeAthletes);
		panelAthletes.add(new JScrollPane(listeAthletes), BorderLayout.CENTER);

		// - boutons (sud)
		JPanel panelButtonsAthlete = new JPanel();
		JLabel labelAthletes = new JLabel("Athletes : ");
		labelAthletes.setFont(new Font(labelAthletes.getFont().getName(), Font.BOLD, LABEL_SIZE));
		panelButtonsAthlete.add(labelAthletes);
		JButton plusAthlete = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("add.gif")));
		plusAthlete.setToolTipText("ajoute un nouvel athlete dans la base de donnees");
		ControlJButtonPlusAthlete controlPlusAthlete = new ControlJButtonPlusAthlete(
				this, this.modele);
		plusAthlete.addActionListener(controlPlusAthlete);
		panelButtonsAthlete.add(plusAthlete);
		JButton moinsAthlete = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("delete.gif")));
		moinsAthlete.setToolTipText("supprimer l'athlete de la base de donnees");
		ControlJButtonDeleteAthlete controlDeleteAthlete = new ControlJButtonDeleteAthlete(
				this, this.modele, moinsAthlete);
		moinsAthlete.addActionListener(controlDeleteAthlete);
		this.modele.addObserver(controlDeleteAthlete);
		panelButtonsAthlete.add(moinsAthlete);
		JButton editAthlete = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("edit.gif")));
		editAthlete.setToolTipText("modifier les infos de l'athlete");
		ControlJButtonEditAthlete controlEditAthlete = new ControlJButtonEditAthlete(
				this, this.modele, editAthlete);
		editAthlete.addActionListener(controlEditAthlete);
		this.modele.addObserver(controlEditAthlete);
		panelButtonsAthlete.add(editAthlete);

		panelButtonsAthlete.add(Box.createHorizontalStrut(50));

		JLabel labelResultats = new JLabel("Resultats : ");
		labelResultats.setFont(new Font(labelResultats.getFont().getName(), Font.BOLD, LABEL_SIZE));
		panelButtonsAthlete.add(labelResultats);
		JButton editResultat = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("edit.gif")));
		editResultat.setToolTipText("modifier la valeur");
		ControlJButtonEditResultat controlEditResultat = new ControlJButtonEditResultat(
				this, this.modele, editResultat);
		editResultat.addActionListener(controlEditResultat);
		this.modele.addObserver(controlEditResultat);
		panelButtonsAthlete.add(editResultat);

		JButton validation = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("lock.gif")));
		validation.setToolTipText("verrouiller les valeurs");
		validation.addActionListener(new ControlJButtonValidateResultats(this,
				this.modele));
		panelButtonsAthlete.add(validation);

		JButton importation = new JButton(new ImageIcon(this.getClass().getClassLoader().getResource("file.gif")));
		importation.setToolTipText("importer des valeurs");
		importation.addActionListener(new ControlJButtonImportResultats(this,
				this.modele));
		panelButtonsAthlete.add(importation);

		panelAthletes.add(panelButtonsAthlete, BorderLayout.SOUTH);
		this.getContentPane().add(panelAthletes, BorderLayout.CENTER);
	}

}
