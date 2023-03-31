package totalVisualization;

import java.io.File;
import java.io.IOException;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.ScrollPane.ScrollBarPolicy;
import javafx.scene.control.TextArea;
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
		Button button = new Button("Visualize Tree");
		button.setFont(new Font(24));
		button.setPrefWidth(350);
		button.setPrefHeight(75);
		button.setOnMouseClicked(e -> {
			selectedProgram = "Tree";
		});
		Button button2 = new Button("Visualize Hypercube");
		button2.setPrefWidth(350);
		button2.setPrefHeight(75);
		button2.setFont(new Font(24));
		button2.setOnMouseClicked(e -> {
			selectedProgram = "Hypercube";
		});
		Button button3 = new Button("Statistics of Attribute Order");
		button3.setPrefWidth(350);
		button3.setPrefHeight(75);
		button3.setFont(new Font(24));
		button3.setOnMouseClicked(e -> {
			selectedProgram = "AttributePieChart";
		});
		Button button4 = new Button("Statistics of Thread");
		button4.setPrefWidth(350);
		button4.setPrefHeight(75);
		button4.setFont(new Font(24));
		button4.setOnMouseClicked(e -> {
			selectedProgram = "ThreadPieChart";
		});
		VBox rightButtons = new VBox();
		leftButtons.setTranslateY(42);
		leftButtons.setSpacing(10);
		rightButtons.setTranslateY(42);
		rightButtons.setSpacing(10);
		Button fileOpener = new Button("Select Log File");
		ObservableList<String> options = FXCollections.observableArrayList("3-clique", "4-clique", "5-clique",
				"3-cycle", "4-cycle", "5-cycle");
		ComboBox combo = new ComboBox(options);
		combo.setPromptText("Premade Queries");
		combo.setPrefWidth(350);
		combo.setPrefHeight(75);
		Text fileName = new Text("Not Selected");
		fileName.setFont(new Font(32));
		fileOpener.setFont(new Font(36));
		fileOpener.setPrefWidth(350);
		fileOpener.setPrefHeight(75);
		rightButtons.getChildren().add(fileOpener);
		rightButtons.getChildren().add(combo);
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

		VBox center = new VBox();
		TextArea queryArea = new TextArea();
		queryArea.setWrapText(true);
		ScrollPane s = new ScrollPane();
		s.setContent(queryArea);
		s.setHbarPolicy(ScrollBarPolicy.NEVER);
		s.setVbarPolicy(ScrollBarPolicy.ALWAYS);
		queryArea.setPromptText("Enter Custom Queries Here");
		queryArea.setFont(new Font(36));
		HBox centerText = new HBox();
		center.getChildren().add(centerText);
		center.getChildren().add(queryArea);
		queryArea.setStyle("-fx-focus-color: black;");
//		queryArea.setFocusTraversable(false);
		queryArea.setPrefHeight(800);
		Text filePrompt = new Text("Log File: ");
		filePrompt.setFont(new Font(32));
		centerText.getChildren().addAll(filePrompt, fileName);
		centerText.setAlignment(Pos.TOP_CENTER);

		leftButtons.getChildren().addAll(button, button2, button3, button4);

		HBox bottom = new HBox();
		Button bottomStart = new Button("Start");
		bottomStart.setOnMouseClicked(e -> {

			if (selectedProgram == "AttributePieChart") {
				Platform.runLater(new Runnable() {
					public void run() {
						try {
							(new visualizationAttributePieChart.HelloFX()).start(new Stage());
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			} else if (selectedProgram == "ThreadPieChart") {
				Platform.runLater(new Runnable() {
					public void run() {
						try {
							(new visualizationThreadPieChart.HelloFX()).start(new Stage());
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
		scene.getStylesheets().add(getClass().getResource("application.css").toExternalForm());
		primaryStage.setWidth(1920);
		primaryStage.setHeight(1000);
		primaryStage.setScene(scene);
		primaryStage.show();
	}
}
