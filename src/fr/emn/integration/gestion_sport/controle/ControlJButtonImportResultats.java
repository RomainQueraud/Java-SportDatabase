package fr.emn.integration.gestion_sport.controle;

import java.awt.Frame;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.filechooser.FileFilter;

import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJButtonImportResultats implements ActionListener {

	private Sport modele;
	private Frame parent;

	public ControlJButtonImportResultats(Frame parent, Sport modele) {
		this.modele = modele;
		this.parent = parent;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		JFileChooser choixFichier = new JFileChooser(".");
		choixFichier.setFileFilter(new FiltreTexte());

		int reponse = choixFichier.showOpenDialog(parent);

		if (reponse == JFileChooser.APPROVE_OPTION)
			try {
				if (modele.importResultatsFile(choixFichier.getSelectedFile()))
					JOptionPane.showMessageDialog(parent, "Importation correcte!", "Succès!",
							JOptionPane.INFORMATION_MESSAGE);
			} catch (Exception ex) {
				ex.printStackTrace();
				JOptionPane.showMessageDialog(parent, "On ne peut pas!", "Erreur!",
						JOptionPane.ERROR_MESSAGE);
			}
	}

}

class FiltreTexte extends FileFilter {
	public boolean accept(File f) {
		return f.isDirectory() || f.getName().toLowerCase().endsWith(".xml");
	}

	public String getDescription() {
		return "Fichiers XML";
	}
}