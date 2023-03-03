package visualizationHyperCube;

import java.io.IOException;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;

public class HelloFX extends Application {

	@Override
	public void start(Stage stage) throws IOException {
		HBox node = new HBox();
		Scene scene = new Scene(node, 320, 240);
		stage.setTitle("Testing Title!");
		stage.setScene(scene);
		stage.show();
	}

	public static void main(String[] args) {
		launch();
	}
}
