package visualizationHyperCube;

import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.Text;
import javafx.scene.text.TextAlignment;
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
		vbox.setAlignment(Pos.CENTER);
		attributeText = new Text("a" + Integer.toString(attributeNum));
		attributeText.setFont(new Font(32));
		attributeText.setFill(Color.RED);
		attributeText.setTextAlignment(TextAlignment.CENTER);
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

	/**
	 * Removes all threads with threadNum in all Chunks of the AttributeBox. Adds a
	 * thread to all chunks that overlap in range.
	 * 
	 * @Precondition: lowerRange must be less than or equal to upperRange.
	 * 
	 * @param threadNum  The number of the thread.
	 * @param lowerRange The lower access range of the thread.
	 * @param upperRange The higher access range of the thread.
	 */
	public void addThread(int threadNum, int lowerRange, int upperRange) {
		if (lowerRange > upperRange)
			return;

		for (Node chunk : vbox.getChildren()) {
			Chunk temp = (Chunk) chunk;
			temp.remove(threadNum);

			if (lowerRange <= temp.upperRange() && temp.lowerRange() <= upperRange) {
				temp.add(threadNum);
			}
		}
	}

	public int getLowerBound() {
		return lowerBound;
	}

	public int getUpperBound() {
		return upperBound;

	}
}
