package totalVisualization;

import java.io.File;
import java.io.IOException;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

/**
 * Main JavaFX code
 * 
 * @author Mitchell Gray
 *
 */
public class HelloFX extends Application {
	private String selectedProgram = "";

	public static String globalFileName = "src/visualizationHyperCube/run4_budget.txt";

	/**
	 * Updates the scene every time the `time` gets to 9. `time` is dependent on
	 * `speed`
	 */
	public void update() {

	}

	public static void main(String[] args) {
		launch();
	}

	@Override
	public void start(Stage primaryStage) throws Exception {
		BorderPane pane = new BorderPane();

		VBox leftButtons = new VBox();
		leftButtons.setAlignment(Pos.TOP_LEFT);
		Button button = new Button("Tree");
		button.setFont(new Font(24));
		button.setPrefWidth(350);
		button.setPrefHeight(75);
		button.setOnMouseClicked(e -> {
			selectedProgram = "Tree";
		});
		Button button2 = new Button("Hypercube");
		button2.setPrefWidth(350);
		button2.setPrefHeight(75);
		button2.setFont(new Font(24));
		button2.setOnMouseClicked(e -> {
			selectedProgram = "Hypercube";
		});
		Button button3 = new Button("PieChart");
		button3.setPrefWidth(350);
		button3.setPrefHeight(75);
		button3.setFont(new Font(24));
		button3.setOnMouseClicked(e -> {
			selectedProgram = "PieChart";
		});
		VBox rightButtons = new VBox();
		Button fileOpener = new Button("Select Log File");
		Text fileName = new Text("Not Selected");
		fileName.setFont(new Font(32));
		fileOpener.setFont(new Font(36));
		fileOpener.setPrefWidth(350);
		fileOpener.setPrefHeight(75);
		rightButtons.getChildren().add(fileOpener);
		fileOpener.setOnMouseClicked(e -> {
			FileChooser fileChooser = new FileChooser();
			fileChooser.setTitle("Open Log File");
			File filePath;
			filePath = fileChooser.showOpenDialog(primaryStage);
			String temp;
			globalFileName = filePath.getAbsolutePath();
			temp = filePath != null ? filePath.getName() : "File Selection Error";
			fileName.setText(temp);
		});

		HBox center = new HBox();
		Text filePrompt = new Text("Log File: ");
		filePrompt.setFont(new Font(32));
		center.getChildren().addAll(filePrompt, fileName);
		center.setAlignment(Pos.TOP_CENTER);

		leftButtons.getChildren().addAll(button, button2, button3);

		HBox bottom = new HBox();
		Button bottomStart = new Button("Start");
		bottomStart.setOnMouseClicked(e -> {

			if (selectedProgram == "PieChart") {
				Platform.runLater(new Runnable() {
					public void run() {
						try {
							(new visualizationPieChart.HelloFX()).start(new Stage());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			} else if (selectedProgram == "Hypercube") {
				Platform.runLater(new Runnable() {

					public void run() {
						try {
							(new visualizationHyperCube.HelloFX()).start(new Stage());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			} else if (selectedProgram == "Tree") {

				Thread thread = new Thread() {
					public void run() {
//						System.out.println("Tree start");
						visualization.VisualRunner.main(new String[0]);
					}
				};
				thread.start();
			}
//			if (selectedProgram.equals("Tree")) {
//				visualization.VisualRunner.main(new String[0]);
//			} else if (selectedProgram.equals("Hypercube")) {
//				visualizationHyperCube.GUIStarter.main(new String[0]);
//			} else if (selectedProgram.equals("PieChart")) {
//				visualizationPieChart.GUIStarter.main(new String[0]);
//			}
		});
		bottomStart.setFont(new Font(32));
		bottomStart.setPrefWidth(350);
		bottomStart.setPrefHeight(75);
		bottom.getChildren().add(bottomStart);

		pane.setLeft(leftButtons);
		pane.setRight(rightButtons);
		pane.setCenter(center);
		pane.setBottom(bottomStart);
		Scene scene = new Scene(pane);
		primaryStage.setWidth(1920);
		primaryStage.setHeight(1000);
		primaryStage.setScene(scene);
		primaryStage.show();
	}
}
