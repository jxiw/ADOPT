package visualizationHyperCube;

import java.io.IOException;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import scala.util.Random;

/**
 * Main JavaFX code
 * 
 * @author Mitchell Gray
 *
 */
public class HelloFX extends Application {

	private int counter = 10;

	@Override
	public void start(Stage stage) throws IOException {
		BorderPane root = new BorderPane();
		Chunk chunk = new Chunk(stage);
		root.setCenter(chunk);
//		root.maxWidth(320);
//		root.maxHeight(240);
//		root.getChildren().add(node);
		Scene scene = new Scene(root, 1280, 720);
		stage.setTitle("Testing Title!");
		stage.setScene(scene);
		stage.show();

		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				Random ran = new Random();
				if (now % 5000 == 0 && counter >= 0) {
					counter--;
					chunk.add(ran.nextInt(100));
				}
			}
		};
		animationTimer.start();
	}

	public static void main(String[] args) {
		launch();
	}
}
