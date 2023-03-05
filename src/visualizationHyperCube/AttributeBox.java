package visualizationHyperCube;

import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.stage.Stage;

/**
 * Represents an aggregation of Chunks
 * 
 * @author Mitchell Gray
 *
 */
public class AttributeBox extends Pane {
	private VBox vbox;
	private Text lowerBoundText;
	private Text upperBoundText;
	private final int lowerBound;
	private final int upperBound;

	/**
	 * Constructor for AttributeBox.
	 * 
	 * @param stage      The primary stage of the application.
	 * @param lowerBound The lowerbound to display on the upper left of the box.
	 * @param upperBound The upperbound to display on the bottom left of the box,
	 */
	public AttributeBox(Stage stage, int lowerBound, int upperBound) {
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		vbox = new VBox();
		lowerBoundText = new Text(Integer.toString(lowerBound));
		upperBoundText = new Text(Integer.toString(upperBound));
		getChildren().addAll(lowerBoundText, vbox, upperBoundText);

	}

	/**
	 * Gets the Chunk object at index `index` of the box.
	 * 
	 * @param index index of the Chunk object to grab.
	 * @return
	 */
	public Chunk getChunk(int index) {
		return (Chunk) vbox.getChildren().get(index);
	}

	public void addChunk(Chunk chunk) {
		vbox.getChildren().add(chunk);
	}

}
