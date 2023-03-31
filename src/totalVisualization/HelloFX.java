package totalVisualization;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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
 */
public class HelloFX extends Application {
	private ArrayList<Button> totalButtons = new ArrayList<Button>();
	private String selectedProgram = "";

	public static String globalFileName = "src/visualizationHyperCube/run4_budget.txt";

	// private final static Pattern sqlPattern = Pattern.compile("(SELECT .*?)");

	public static String runQuery = "";

	HashMap<String, String> queries = new HashMap<>();

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
		Button button = new Button("Search Tree View");
		button.setFont(new Font(24));
		button.setPrefWidth(350);
		button.setPrefHeight(75);
		button.setOnMouseClicked(e -> {
			selectedProgram = "Tree";

			totalButtons.forEach((b) -> {
				b.setId("");
			});

			button.setId("selected");
		});
		Button button2 = new Button("Attribute Value View");
		button2.setPrefWidth(350);
		button2.setPrefHeight(75);
		button2.setFont(new Font(24));
		button2.setOnMouseClicked(e -> {
			selectedProgram = "Hypercube";
			totalButtons.forEach((b) -> {
				b.setId("");
			});
			button2.setId("selected");
		});
		Button button3 = new Button("Per-Order Breakdown");
		button3.setPrefWidth(350);
		button3.setPrefHeight(75);
		button3.setFont(new Font(24));
		button3.setOnMouseClicked(e -> {
			selectedProgram = "AttributePieChart";

			totalButtons.forEach((b) -> {
				b.setId("");
			});

			button3.setId("selected");
		});
		Button button4 = new Button("Per-Thread Breakdown");
		button4.setPrefWidth(350);
		button4.setPrefHeight(75);
		button4.setFont(new Font(24));
		button4.setOnMouseClicked(e -> {
			selectedProgram = "ThreadPieChart";

			totalButtons.forEach((b) -> {
				b.setId("");
			});

			button4.setId("selected");
		});
		VBox rightButtons = new VBox();
		leftButtons.setTranslateY(42);
		leftButtons.setSpacing(10);
		rightButtons.setTranslateY(42);
		rightButtons.setSpacing(10);
		Button fileOpener = new Button("Select Log File");
		ObservableList<String> options = FXCollections.observableArrayList("3-clique", "4-clique", "5-clique",
				"6-clique", "7-clique", "3-cycle", "4-cycle", "5-cycle", "6-cycle", "7-cycle");
		ComboBox combo = new ComboBox(options);
		combo.setPromptText("Example Graph Queries");
		combo.setPrefWidth(350);
		combo.setPrefHeight(75);
		Text fileName = new Text("Not Selected");
		fileName.setFont(new Font(32));
		fileOpener.setFont(new Font(36));
		fileOpener.setPrefWidth(350);
		fileOpener.setPrefHeight(75);
		rightButtons.getChildren().add(fileOpener);
		rightButtons.getChildren().add(combo);

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
		totalButtons.add(button);
		totalButtons.add(button2);
		totalButtons.add(button3);
		totalButtons.add(button4);

		fileOpener.setOnMouseClicked(e -> {
			FileChooser fileChooser = new FileChooser();
			fileChooser.setTitle("Open Log File");
			File filePath;
			filePath = fileChooser.showOpenDialog(primaryStage);
			String temp;
			globalFileName = filePath.getAbsolutePath();
			temp = filePath != null ? filePath.getName() : "File Selection Error";
			fileName.setText(temp);
			// read and parse queries.
			try {
				BufferedReader br = new BufferedReader(new FileReader(globalFileName));
				String line = br.readLine();
				while (line != null) {
					// extract query here.
					if (line.toLowerCase().startsWith("select")) {
						queryArea.setText(line);
						break;
					}
					line = br.readLine();
				}
				br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		});

		queries.put("3-clique",
				"select count(*) from edge e1, edge e2, edge e3 where e1.tid=e2.sid and e2.tid=e3.tid and e3.sid = e1.sid "
						+ "and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid;");
		queries.put("3-cycle",
				"select count(*) from edge e1, edge e2, edge e3 where e1.tid=e2.sid and e2.tid=e3.tid and e3.sid = e1.sid "
						+ "and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid;");
		queries.put("4-clique",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6 WHERE e1.tid = e2.sid and e2.tid = e3.sid and e3.tid = e4.sid and e4.tid = e1.sid "
						+ "and e1.sid = e5.sid and e2.tid = e5.tid and e2.sid = e6.sid and e3.tid = e6.tid and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid;");
		queries.put("4-cycle",
				"select count(*) from edge e1, edge e2, edge e3, edge e4 where e1.tid=e2.sid and e2.tid=e3.sid and e3.tid = e4.sid and e4.tid = e1.sid and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid;");
		queries.put("5-clique",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9, edge e10 "
						+ "WHERE e1.tid = e2.sid and e2.tid = e3.sid and e3.tid = e4.sid and e4.tid = e5.tid and e1.sid = e5.sid "
						+ "and e6.sid = e1.sid and e6.tid = e2.tid " + "and e7.sid = e1.sid and e7.tid = e3.tid "
						+ "and e8.sid = e2.sid and e8.tid = e3.tid " + "and e9.sid = e2.sid and e9.tid = e4.tid "
						+ "and e10.sid = e3.sid and e10.tid = e4.tid "
						+ "and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid and e4.sid < e4.tid;");
		queries.put("5-cycle", "SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5 "
				+ "WHERE e1.tid = e2.sid and e2.tid = e3.sid and e3.tid = e4.sid and e4.tid = e5.tid and e1.sid = e5.sid "
				+ "and e1.sid < e1.tid and e2.sid < e2.tid and e3.sid < e3.tid and e4.sid < e4.tid;");
		queries.put("6-clique",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9, "
						+ "edge e10, edge e11, edge e12, edge e13, edge e14, edge e15 "
						+ "where e1.tid = e2.sid AND e2.tid = e3.sid AND e3.tid = e4.sid AND e4.tid = e5.sid AND e1.sid = e6.sid AND e5.tid = e6.tid "
						+ "AND e1.sid = e7.sid and e7.tid = e2.tid AND e1.sid = e8.sid and e8.tid = e3.tid and e1.sid = e9.sid and e9.tid = e4.tid "
						+ "AND e2.sid = e10.sid and e10.tid = e3.tid AND e2.sid = e11.sid and e11.tid = e4.tid AND e2.sid = e12.sid and e12.tid = e5.tid "
						+ "AND e3.sid = e13.sid and e13.tid = e4.tid AND e3.sid = e14.sid AND e14.tid = e5.tid "
						+ "AND e4.sid = e15.sid and e15.tid = e5.tid AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid "
						+ "AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid AND e7.sid < e7.tid AND e8.sid < e8.tid "
						+ "AND e9.sid < e9.tid   AND e10.sid < e10.tid AND e11.sid < e11.tid AND e12.sid < e12.tid "
						+ "AND e13.sid < e13.tid AND e14.sid < e14.tid AND e15.sid < e15.tid;");
		queries.put("6-cycle",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6 where e1.tid = e2.sid AND e2.tid = e3.sid "
						+ "AND e3.tid = e4.sid AND e4.tid = e5.sid AND e1.sid = e6.sid AND e5.tid = e6.tid AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid "
						+ "AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid; ");
		queries.put("7-clique",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7, edge e8, edge e9, "
						+ "edge e10, edge e11, edge e12, edge e13, edge e14, edge e15, edge e16, edge e17, edge e18, edge e19, edge e20, edge e21 "
						+ "where e1.sid = e7.sid AND e1.sid = e8.sid AND e1.sid = e9.sid AND e1.sid = e10.sid AND e1.sid = e11.sid "
						+ "AND e1.tid = e2.sid AND e1.tid = e12.sid AND e1.tid = e13.sid AND e1.tid = e14.sid AND e1.tid = e15.sid "
						+ "AND e2.tid = e3.sid AND e2.tid = e7.tid AND e2.tid = e16.sid AND e2.tid = e17.sid AND e2.tid = e18.sid "
						+ "AND e3.tid = e4.sid AND e3.tid = e8.tid AND e3.tid = e12.tid AND e3.tid = e19.sid AND e3.tid = e20.sid "
						+ "AND e4.tid = e5.sid AND e4.tid = e9.tid AND e4.tid = e13.tid AND e4.tid = e16.tid AND e4.tid = e21.sid "
						+ "AND e5.tid = e6.sid AND e5.tid = e10.tid AND e5.tid = e14.tid AND e5.tid = e17.tid AND e5.tid = e19.tid "
						+ "AND e6.tid = e11.tid AND e6.tid = e15.tid AND e6.tid = e18.tid AND e6.tid = e20.tid AND e6.tid = e21.tid "
						+ "AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid "
						+ "AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid "
						+ "AND e7.sid < e7.tid AND e8.sid < e8.tid AND e9.sid < e9.tid "
						+ "AND e10.sid < e10.tid AND e11.sid < e11.tid AND e12.sid < e12.tid "
						+ "AND e13.sid < e13.tid AND e14.sid < e14.tid AND e15.sid < e15.tid "
						+ "AND e16.sid < e16.tid AND e17.sid < e17.tid AND e18.sid < e18.tid "
						+ "AND e19.sid < e19.tid AND e20.sid < e20.tid AND e21.sid < e21.tid;");
		queries.put("7-cycle",
				"SELECT count(*) FROM edge e1, edge e2, edge e3, edge e4, edge e5, edge e6, edge e7 where e1.tid = e2.sid AND e2.tid = e3.sid "
						+ "AND e3.tid = e4.sid AND e4.tid = e5.sid AND e5.tid = e6.sid AND e6.tid = e7.tid AND e1.sid = e7.sid "
						+ "AND e1.sid < e1.tid AND e2.sid < e2.tid AND e3.sid < e3.tid "
						+ "AND e4.sid < e4.tid AND e5.sid < e5.tid AND e6.sid < e6.tid " + "AND e7.sid < e7.tid;");

		combo.setOnAction(e -> {
			String selectedOption = (String) combo.getSelectionModel().getSelectedItem();
			System.out.println("Selected option: " + selectedOption);
			queryArea.setText(queries.get(selectedOption));
		});

		HBox bottom = new HBox();
		Button bottomStart = new Button("Start");
		bottomStart.setOnMouseClicked(e -> {
			runQuery = queryArea.getText();
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