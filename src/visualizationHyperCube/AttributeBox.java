package visualizationHyperCube;

import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.stage.Stage;

//TODO: Add functionality that lets clients add threads with a specified range of 2 ints.
/**
 * Represents an aggregation of Chunks
 * 
 * @author Mitchell Gray
 *
 */
public class AttributeBox extends VBox {
	private VBox vbox;
	private Text attributeText;
	private Text lowerBoundText;
	private Text upperBoundText;
	private final int attributeNum;
	private final int lowerBound;
	private final int upperBound;

	/**
	 * Constructor for AttributeBox.
	 * 
	 * @param stage      The primary stage of the application.
	 * @param lowerBound The lowerbound to display on the upper left of the box.
	 * @param upperBound The upperbound to display on the bottom left of the box,
	 */
	public AttributeBox(Stage stage, int lowerBound, int upperBound, int attributeNum) {
		this.attributeNum = attributeNum;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		vbox = new VBox();
		attributeText = new Text("a" + Integer.toString(attributeNum));
		attributeText.setFont(new Font(32));
		attributeText.setFill(Color.RED);
		attributeText.setTranslateX(Chunk.CHUNK_WIDTH / 2);
		lowerBoundText = new Text("Lower Bound: " + Integer.toString(lowerBound));
		upperBoundText = new Text("Upper Bound: " + Integer.toString(upperBound));
		getChildren().addAll(attributeText, lowerBoundText, vbox, upperBoundText);

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
