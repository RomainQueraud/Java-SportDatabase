package fr.emn.integration.gestion_sport;

import fr.emn.integration.gestion_sport.abstraction.BDSport;
import fr.emn.integration.gestion_sport.abstraction.Sport;
import fr.emn.integration.gestion_sport.abstraction.SportJDBC;
import fr.emn.integration.gestion_sport.presentation.MainWindow;

public class GestionSport {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BDSport.connexionBD();
		
		Sport disciplines = new SportJDBC();

		MainWindow fen = new MainWindow(disciplines);
		fen.setVisible(true);
	}

}
