package visualizationHyperCube;

import java.io.IOException;
import java.util.Random;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

public class HelloFX extends Application {

	@Override
	public void start(Stage stage) throws IOException {
		HBox node = new HBox();
		node.setMaxWidth(320);

//		Rectangle rect1 = new Rectangle();
//		Rectangle rect2 = new Rectangle();
//		Rectangle rect3 = new Rectangle();
//		node.getChildren().add(rect1);
//		node.getChildren().add(rect2);
//		node.getChildren().add(rect3);
//		rect1.setFill(Color.RED);
//		rect1.setWidth(node.getMaxWidth() / node.getChildren().size());
//		rect1.setHeight(100);
//		rect2.setFill(Color.BLUE);
//		rect2.setWidth(node.getMaxWidth() / node.getChildren().size());
//		rect2.setHeight(100);
//		rect3.setFill(Color.YELLOW);
//		rect3.setWidth(node.getMaxWidth() / node.getChildren().size());
//		rect3.setHeight(100);
		Scene scene = new Scene(node, 320, 240);
		stage.setTitle("Testing Title!");
		stage.setScene(scene);
		stage.show();

		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				if (now % 100 == 0) {

					Rectangle temp = new Rectangle();
					Random random = new Random();
					Color color = Color.BLACK;

					switch (random.nextInt(4)) {
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
					}
					temp.setFill(color);
					temp.setHeight(100);
					temp.setWidth(node.getMaxWidth() / node.getChildren().size() + 1);

					node.getChildren().add(temp);
					node.getChildren().forEach((Node n) -> {
						Rectangle rect = (Rectangle) n;
						rect.setWidth(node.getMaxWidth() / node.getChildren().size());
					});

//					node.getChildren().sort(new Comparator<Node>() {
//
//						@Override
//						public int compare(Node o1, Node o2) {
//							if (o1.hashCode() == o2.hashCode()) {
//								return 0;
//							} else if (o1.hashCode() > o2.hashCode()) {
//								return 1;
//							} else {
//								return -1;
//							}
//
//						}
//
//					});
				}
			}
		};

		animationTimer.start();
	}

	public static void main(String[] args) {
		launch();
	}
}
