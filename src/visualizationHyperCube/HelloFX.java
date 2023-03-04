package visualizationHyperCube;

import java.io.IOException;
import java.util.Random;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.BorderPane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

public class HelloFX extends Application {

	private int counter = 0;

	@Override
	public void start(Stage stage) throws IOException {
		BorderPane root = new BorderPane();
		Chunk chunk = new Chunk(stage, 100, 200);
		root.setCenter(chunk.getHBox());
//		root.maxWidth(320);
//		root.maxHeight(240);
//		root.getChildren().add(node);
		Scene scene = new Scene(root, 320, 240);
		stage.setTitle("Testing Title!");
		stage.setScene(scene);
		stage.show();

		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				if (now % 10000 == 0 && counter < 10) {

					Rectangle temp = new Rectangle();
					Random random = new Random();
					Color color = Color.BLACK;
					random.nextInt(4);

					switch (counter % 5) {

					case 0:
						color = Color.RED;
						break;
					case 1:
						color = Color.BLUE;
						break;
					case 2:
						color = Color.YELLOW;
						break;
					case 3:
						color = Color.GREEN;
						break;
					case 4:
						color = Color.PINK;
						break;
					}

					counter++;
					temp.setFill(color);
					temp.setHeight(Chunk.CHUNK_HEIGHT - 2 * Chunk.BORDER_SIZE);
					chunk.getHBox().getChildren().forEach((n) -> {
						Rectangle rect = (Rectangle) n;

						rect.setWidth(Math.floor((double) ((Chunk.CHUNK_WIDTH - 2. * Chunk.BORDER_SIZE)
								/ (chunk.getHBox().getChildren().size() + 1))));
					});
					chunk.getHBox().getChildren().add(temp);
					temp.setWidth(1);
					temp.setWidth(
							(Chunk.CHUNK_WIDTH - 2 * Chunk.BORDER_SIZE) - ((chunk.getHBox().getChildren().size() - 1)
									* ((Rectangle) (chunk.getHBox().getChildren().get(0))).getWidth()));
				}
			}
		};
		animationTimer.start();
	}

	public static void main(String[] args) {
		launch();
	}
}
