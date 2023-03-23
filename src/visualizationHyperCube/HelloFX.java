package visualizationHyperCube;

import java.io.IOException;
import java.util.ArrayList;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Slider;
import javafx.scene.input.KeyCode;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;

/**
 * Main JavaFX code
 * 
 * @author Mitchell Gray
 *
 */
public class HelloFX extends Application {
	private final String FILE_PATH = "src/visualizationHyperCube/run4_budget.txt";
	private final int NUMBER_OF_CHUNKS = 70;
	public static Text threadText;
	public static Text chunkText;

	private double time = 0;
	private int counter = 15;
	private int speed = 50;
	private int tempSpeed = -1;

	private boolean paused;

	private ArrayList<Chunk> chunkList1;
	private ArrayList<Chunk> chunkList2;
	private ArrayList<Chunk> chunkList3;
	private AttributeBox boxOne;
	private AttributeBox boxTwo;
	private AttributeBox boxThree;
	private DataParser parser;

	@Override
	public void start(Stage stage) throws IOException {
		parser = new DataParser(FILE_PATH);
		int[] bounds = parser.getBounds();

		// Speed Slider
		VBox bottom = new VBox();
		Slider slide = new Slider();
		Text bottomText = new Text();
		slide.setShowTickLabels(true);
		slide.setShowTickMarks(true);
		slide.setBlockIncrement(1);
		slide.adjustValue(50);
		bottomText.setText("Speed: " + speed + "%");
		bottomText.setFont(new Font(32));
		bottom.getChildren().addAll(bottomText, slide);
		slide.valueProperty().addListener(new ChangeListener<Number>() {
			public void changed(ObservableValue<? extends Number> ov, Number old_val, Number new_val) {
				if (paused) {
					slide.setDisable(true);
					return;
				} else {
					slide.setDisable(false);
				}

				speed = new_val.intValue();
				bottomText.setText("Speed: " + speed + "%");
			}
		});

		// Upper Text
		chunkText = new Text();
		chunkText.setFont(new Font(36));
		chunkText.setText("Chunk Range: (None) ");
		threadText = new Text();
		threadText.setFont(new Font(36));
		threadText.setText("Thread Number: (None) ");
		HBox textHolder = new HBox();
		textHolder.getChildren().addAll(chunkText, threadText);

		// Center Attribute Boxes
		HBox boxHolder = new HBox();
		boxOne = new AttributeBox(stage, bounds[0], bounds[1], 1);
		boxTwo = new AttributeBox(stage, bounds[2], bounds[3], 2);
		boxThree = new AttributeBox(stage, bounds[4], bounds[5], 3);
		boxOne.setTranslateX(50);
		boxThree.setTranslateX(-50);
		boxOne.setAlignment(Pos.CENTER);
		boxTwo.setAlignment(Pos.CENTER);
		boxThree.setAlignment(Pos.CENTER);
		boxHolder.getChildren().addAll(boxOne, boxTwo, boxThree);

		// Creates set number of chunks and assigns that many to each chunk.
		chunkList1 = new ArrayList<Chunk>();
		chunkList2 = new ArrayList<Chunk>();
		chunkList3 = new ArrayList<Chunk>();

		double difference = boxOne.getUpperBound() - boxOne.getLowerBound();
		double multiplier = difference / NUMBER_OF_CHUNKS;
		for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
			chunkList1.add(new Chunk(stage, (int) Math.floor(i * multiplier + boxOne.getLowerBound()),
					(int) Math.floor(i * multiplier + multiplier - 1 + boxOne.getLowerBound())));
			chunkList2.add(new Chunk(stage, (int) Math.floor(i * multiplier + boxOne.getLowerBound()),
					(int) Math.floor(i * multiplier + multiplier - 1 + boxOne.getLowerBound())));
			chunkList3.add(new Chunk(stage, (int) Math.floor(i * multiplier + boxOne.getLowerBound()),
					(int) Math.floor(i * multiplier + multiplier - 1 + boxOne.getLowerBound())));
		}

		chunkList1.forEach((chunk) -> {
			boxOne.addChunk(chunk);
		});
		chunkList2.forEach((chunk) -> {
			boxTwo.addChunk(chunk);
		});
		chunkList3.forEach((chunk) -> {
			boxThree.addChunk(chunk);
		});

		// Setting up the root, scene, and stage
//		Pane root = new Pane();
		BorderPane bp = new BorderPane();
//		BorderPane.setMargin(bp, new Insets(100, 100, 100, 100));
		bp.setTop(textHolder);
		bp.setLeft(boxOne);
		bp.setCenter(boxTwo);
		bp.setRight(boxThree);
		bp.setBottom(bottom);
//		root.getChildren().add(bp);

		Scene scene = new Scene(bp, 1920, 1000);
		stage.setMinWidth(1920);

		// Handles pausing
		scene.setOnKeyPressed((e) -> {
			if (e.getCode() == KeyCode.SPACE && tempSpeed == -1) {
				paused = true;
				tempSpeed = speed;
				slide.setValue(0);
				bottomText.setText("Speed: 0%");
				speed = 0;
			} else if (e.getCode() == KeyCode.SPACE && tempSpeed != -1) {
				paused = false;
				slide.setValue(tempSpeed);
				bottomText.setText("Speed: " + tempSpeed + "%");
				speed = tempSpeed;
				tempSpeed = -1;
			}
		});
		stage.setTitle("ADOPT - Hypercube Visualization");
		stage.setScene(scene);
		stage.show();

		// Update on tick
		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				update();

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
		case 0:
			color = Color.LIGHTSEAGREEN;
			break;
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

	/**
	 * Updates the scene every time the `time` gets to 9. `time` is dependent on
	 * `speed`
	 */
	public void update() {
		time += 0.01 * speed;

		if (time >= 9) {
			time = 0;

			int[] data = parser.getNext();

			for (int i = 0; i < chunkList1.size(); i++) {
				boxOne.addThread(data[6], data[0], data[1]);
			}
			for (int i = 0; i < chunkList1.size(); i++) {
				boxTwo.addThread(data[6], data[2], data[3]);
			}
			for (int i = 0; i < chunkList1.size(); i++) {
				boxThree.addThread(data[6], data[4], data[5]);
			}

		}

	}

	public static void main(String[] args) {
		launch();
	}
}
