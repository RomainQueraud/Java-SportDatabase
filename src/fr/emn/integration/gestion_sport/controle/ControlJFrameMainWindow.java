package fr.emn.integration.gestion_sport.controle;

import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

import fr.emn.integration.gestion_sport.abstraction.Sport;

public class ControlJFrameMainWindow implements WindowListener {

	private Sport modele;
	
	public ControlJFrameMainWindow(Sport modele) {
		this.modele = modele;
	}
	
	@Override
	public void windowActivated(WindowEvent arg0) {
		
	}

	@Override
	public void windowClosed(WindowEvent arg0) {
		
	}

	@Override
	public void windowClosing(WindowEvent arg0) {
		modele.close();
	}

	@Override
	public void windowDeactivated(WindowEvent arg0) {
		
	}

	@Override
	public void windowDeiconified(WindowEvent arg0) {
		
	}

	@Override
	public void windowIconified(WindowEvent arg0) {
		
	}

	@Override
	public void windowOpened(WindowEvent arg0) {
		
	}

}
