package visualizationHyperCube;

import javafx.scene.layout.VBox;
import javafx.stage.Stage;

/**
 * Represents an aggregation of Chunks
 * 
 * @author Mitchell Gray
 *
 */
public class AttributeBox extends VBox {
	private VBox vbox;
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

}
