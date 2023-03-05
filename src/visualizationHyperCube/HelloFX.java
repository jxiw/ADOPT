package visualizationHyperCube;

import java.io.IOException;
import java.util.ArrayList;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
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

	private int counter = 15;

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
		AttributeBox boxOne = new AttributeBox(stage, 0, 1000, 1);
		ArrayList<Chunk> chunkList = new ArrayList<Chunk>();
		for (int i = 0; i < 10; i++) {
			chunkList.add(new Chunk(stage, i * 100, i * 100 + 99));
		}

		chunkList.forEach((chunk) -> {
			boxOne.addChunk(chunk);
		});

		root.setTop(textHolder);
		root.setCenter(boxOne);

		stage.setTitle("ADOPT - Hypercube Visualization");
		stage.setScene(scene);
		stage.show();

		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				Random ran = new Random();
				if (now % 5000 == 0 && counter >= 0) {
					counter--;
					for (int i = 0; i < chunkList.size(); i++) {
						chunkList.get(i).add(ran.nextInt(15));
					}
				} else if (now % 500 == 0 && counter < 0) {
					for (int i = 0; i < chunkList.size(); i++) {
						chunkList.get(i).remove(ran.nextInt(15));
					}
				}
			}
		};
		animationTimer.start();
	}

	/**
	 * Maps the thread number to a color.
	 * 
	 * @param num The thread number.
	 * @return The color it mapped to.
	 */
	public static Color getColor(int num) {
		Color color = null;

		switch (num % 27) {
		case 1:
			color = Color.RED;
			break;
		case 2:
			color = Color.GREEN;
			break;
		case 3:
			color = Color.BLUE;
			break;
		case 4:
			color = Color.YELLOW;
			break;
		case 5:
			color = Color.PINK;
			break;
		case 6:
			color = Color.ORANGE;
			break;
		case 7:
			color = Color.PURPLE;
			break;
		case 8:
			color = Color.LIME;
			break;
		case 9:
			color = Color.THISTLE;
			break;
		case 10:
			color = Color.OLIVEDRAB;
			break;
		case 11:
			color = Color.AQUA;
			break;
		case 12:
			color = Color.PERU;
			break;
		case 13:
			color = Color.TOMATO;
			break;
		case 14:
			color = Color.GOLD;
			break;
		case 15:
			color = Color.LIGHTGRAY;
			break;
		case 16:
			color = Color.BLUEVIOLET;
			break;
		case 17:
			color = Color.BROWN;
			break;
		case 18:
			color = Color.CORAL;
			break;
		case 19:
			color = Color.SKYBLUE;
			break;
		case 20:
			color = Color.FUCHSIA;
			break;
		case 21:
			color = Color.VIOLET;
			break;
		case 22:
			color = Color.LIGHTCYAN;
			break;
		case 23:
			color = Color.PLUM;
			break;
		case 24:
			color = Color.YELLOWGREEN;
			break;
		case 25:
			color = Color.LIGHTSALMON;
			break;
		case 26:
			color = Color.POWDERBLUE;
			break;
		}

		return color;
	}

	public static void main(String[] args) {
		launch();
	}
}
