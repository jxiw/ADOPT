package visualizationHyperCube;

import javafx.geometry.Insets;
import javafx.scene.layout.Border;
import javafx.scene.layout.BorderStroke;
import javafx.scene.layout.BorderStrokeStyle;
import javafx.scene.layout.BorderWidths;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

public class Chunk {
	private int lowerBound;
	private int upperBound;
	private HBox hbox;
	public static double BORDER_SIZE = 1.;
	public static double CHUNK_HEIGHT = 100.;
	public static double CHUNK_WIDTH = 102.;

	public Chunk(Stage stage, int lowerBound, int upperBound) {
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		hbox = new HBox();
		hbox.setPadding(new Insets(0, 0, 0, 0));
		HBox.setMargin(hbox, new Insets(0, 0, 0, 0));
		hbox.setSpacing(0);
		hbox.setMaxWidth(CHUNK_WIDTH);
		hbox.setMaxHeight(CHUNK_HEIGHT);
		hbox.setMinWidth(CHUNK_WIDTH);
		hbox.setMinHeight(CHUNK_HEIGHT);
		hbox.setBorder(new Border(new BorderStroke(Color.BLACK, BorderStrokeStyle.SOLID, CornerRadii.EMPTY,
				new BorderWidths(BORDER_SIZE))));
	}

	public HBox getHBox() {
		return hbox;
	}
}
