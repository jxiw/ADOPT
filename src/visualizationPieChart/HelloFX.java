package visualizationPieChart;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;

import javafx.animation.AnimationTimer;
import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.EventHandler;
import javafx.geometry.Side;
import javafx.scene.Scene;
import javafx.scene.chart.PieChart;
import javafx.scene.control.Label;
import javafx.scene.control.Slider;
import javafx.scene.input.KeyCode;
import javafx.scene.input.MouseEvent;
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
	private DataParser parser;
	private double time = 0;
	private int speed = 50;
	private ObservableList<PieChart.Data> pieChartData;
	private ObservableList<PieChart.Data> pieChartData2;
	private Label caption = new Label("");
	private boolean paused;
	private int tempSpeed = -1;
	private double totalData = 0;
	private static final DecimalFormat df = new DecimalFormat("0.00");

	@Override
	public void start(Stage stage) throws IOException {
		parser = new DataParser("src/visualizationHyperCube/run4_budget.txt");
		BorderPane group = new BorderPane();
		group.setStyle("-fx-background-color: White;");
		group.setStyle("-fx-background-color: White;");
		Scene scene = new Scene(group);
		stage.setTitle("Hypercube Attribute Orders");
		stage.setWidth(500);
		stage.setHeight(500);
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

		pieChartData = FXCollections.observableArrayList();
		pieChartData2 = FXCollections.observableArrayList();
//		new PieChart.Data("", 0)
		final PieChart chart = new PieChart(pieChartData);
		final PieChart chart2 = new PieChart(pieChartData2);
		chart.setAnimated(false);
		chart2.setAnimated(false);

		chart2.setTitle("Hypercube Threads");
		chart2.setTitleSide(Side.BOTTOM);
		chart2.setLegendSide(Side.RIGHT);
		caption.setTextFill(Color.BLACK);
		caption.setStyle("-fx-font: 24 arial;");

		chart.setTitle("Hypercube Attribute Orders");
		chart.setLegendSide(Side.LEFT);
		chart.setTitleSide(Side.BOTTOM);
		chart.setStyle("-fx-font-size: " + 28 + "px;");
		chart2.setStyle("-fx-font-size: " + 28 + "px;");
		group.getChildren().addAll(caption);
		HBox center = new HBox();
		center.getChildren().addAll(chart, chart2, caption);
		group.setCenter(center);
		group.setBottom(bottom);

		stage.setScene(scene);
		stage.show();

		AnimationTimer animationTimer = new AnimationTimer() {
			@Override
			public void handle(long now) {
				update();

			}
		};
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
		animationTimer.start();
	}

	public void update() {
		time += 0.01 * speed;

		if (time >= 3) {
			time = 0;

			Object[] data = parser.getNext();
			if ((Double) data[1] < 0) {
				return;
			}

			totalData += (Double) data[1];

			boolean firstChart = false;
			for (int i = 0; i < pieChartData.size(); i++) {
				if (pieChartData.get(i).getName().equals(data[0])) {
					pieChartData.get(i).setPieValue(pieChartData.get(i).getPieValue() + (Double) (data[1]));
					firstChart = true;
				}

			}

			if (!firstChart) {
				PieChart.Data tempData = new PieChart.Data((String) data[0], (Double) (data[1]));
				pieChartData.add(tempData);
				tempData.getNode().addEventHandler(MouseEvent.MOUSE_MOVED, new EventHandler<MouseEvent>() {
					@Override
					public void handle(MouseEvent e) {
						caption.setTranslateX(e.getSceneX() / 3);
						caption.setTranslateY(e.getSceneY() / 3);
						caption.setText(String.valueOf(df.format(tempData.getPieValue() / totalData * 100)) + "%");
					}
				});
			}

			boolean secondChart = false;
			for (int i = 0; i < pieChartData2.size(); i++) {
				if (pieChartData2.get(i).getName().equals(Integer.toString((Integer) data[2]))) {
					pieChartData2.get(i).setPieValue(pieChartData2.get(i).getPieValue() + (Double) (data[1]));
					secondChart = true;
				}

			}

			if (!secondChart) {
				PieChart.Data tempData2 = new PieChart.Data(Integer.toString((Integer) data[2]), (Double) (data[1]));
				pieChartData2.add(tempData2);
//			tempData.getNode()
				tempData2.getNode().addEventHandler(MouseEvent.MOUSE_MOVED, new EventHandler<MouseEvent>() {
					@Override
					public void handle(MouseEvent e) {
						caption.setTranslateX(e.getSceneX() / 3);
						caption.setTranslateY(e.getSceneY() / 3);
						caption.setText(String.valueOf(df.format(tempData2.getPieValue() / totalData * 100)) + "%");
					}
				});
			}
			FXCollections.sort(pieChartData2, new Comparator<PieChart.Data>() {

				@Override
				public int compare(PieChart.Data n1, PieChart.Data n2) {
					if (Integer.parseInt(n1.getName()) == Integer.parseInt(n2.getName())) {
						return 0;
					} else if (Integer.parseInt(n1.getName()) > Integer.parseInt(n2.getName())) {
						return 1;
					} else {
						return -1;
					}
				}
			});

		}

	}

	public static void main(String[] args) {
		launch();
	}
}
