package fr.emn.integration.gestion_sport.abstraction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BDSport {

	private static Connection connexion;

	/**
	 * driver JDBC
	 */
	private static final String jdbcDriver = "org.hsqldb.jdbcDriver";

	/**
	 * fichiers db
	 */
	private static final String database = "jdbc:hsqldb:file:db/sportdb";

	/**
	 * utilisateur qui se connecte à la base de données
	 */
	private static final String user = "sa";

	/**
	 * mot de passe pour se connecter à la base de données
	 */
	private static final String password = "";

	/**
	 * Connexion à la base de donnée
	 */
	public static void connexionBD() {
		try {
			// On commence par charger le driver JDBC d'HyperSQL
			Class.forName(jdbcDriver).newInstance();
		} catch (InstantiationException e) {
			System.out.println("ERROR: failed to load HSQLDB JDBC driver.");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		try {
			// Puis on se connecte à la base de donnees
			connexion = DriverManager.getConnection(database, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Arrête correctement HyperSQL
	 * 
	 * @throws SQLException
	 */
	public static void arretBD() throws SQLException {
		Statement st = connexion.createStatement();
		// On envoie l'instruction pour arreter proprement HSQLDB
		st.execute("SHUTDOWN");
		// On ferme la connexion
		connexion.close();

	}

	public static Connection getConnexion() {
		return connexion;
	}

	public static void main(String[] args) {
		BDSport.connexionBD();
		try {
			System.out.println("creation de la table...");
			Statement s = BDSport.getConnexion().createStatement();
			s.executeUpdate("CREATE TABLE test (nom CHAR(256))");

			System.out.println("destruction de la table...");
			Statement t = BDSport.getConnexion().createStatement();
			t.executeUpdate("DROP TABLE test");

			BDSport.arretBD();
			System.out.println("terminé !");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
