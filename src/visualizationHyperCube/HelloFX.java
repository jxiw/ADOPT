package visualizationHyperCube;

import java.io.IOException;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;
import scala.util.Random;

/**
 * Main JavaFX code
 * 
 * @author Mitchell Gray
 *
 */
public class HelloFX extends Application {

	public static Text threadText;
	public static Text chunkText;

	private int counter = 10;

	@Override
	public void start(Stage stage) throws IOException {
		chunkText = new Text();
		chunkText.setFont(new Font(36));
		chunkText.setText("Chunk Range: (None) ");
		threadText = new Text();
		threadText.setFont(new Font(36));
		threadText.setText("Thread Number: (None) ");
		HBox textHolder = new HBox();
		textHolder.getChildren().addAll(chunkText, threadText);

		BorderPane root = new BorderPane();
		Scene scene = new Scene(root, 1280, 720);
		AttributeBox boxOne = new AttributeBox(stage, 0, 1000);
		Chunk chunk = new Chunk(stage, 0, 99);
		Chunk chunk2 = new Chunk(stage, 100, 199);

		boxOne.addChunk(chunk);
		boxOne.addChunk(chunk2);

		root.setTop(textHolder);
		root.setCenter(boxOne);

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
					chunk2.add(ran.nextInt(100));
				}
			}
		};
		animationTimer.start();
	}

	public static void main(String[] args) {
		launch();
	}
}
