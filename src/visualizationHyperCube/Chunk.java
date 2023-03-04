package visualizationHyperCube;

import java.util.HashMap;

import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.layout.Border;
import javafx.scene.layout.BorderStroke;
import javafx.scene.layout.BorderStrokeStyle;
import javafx.scene.layout.BorderWidths;
import javafx.scene.layout.CornerRadii;
import javafx.scene.layout.HBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.stage.Stage;

/**
 * Represents an aggregation of rows.
 * 
 * @author Mitchell Gray
 *
 */
public class Chunk extends HBox {
	public static double BORDER_SIZE = 1.;
	public static double CHUNK_HEIGHT = 10.;
	public static double CHUNK_WIDTH = 102.;
	public static double THREAD_BORDER_SIZE = 1.;
	public static Color THREAD_BORDER_COLOR = Color.BLACK;

	/**
	 * Keys are threadNums and Values are position in getChildren() List.
	 */
	private HashMap<Integer, Integer> threadMap;

	/**
	 * @param stage The primary stage of the application.
	 */
	public Chunk(Stage stage) {
		threadMap = new HashMap<Integer, Integer>();
		setPadding(new Insets(0, 0, 0, 0));
		HBox.setMargin(this, new Insets(0, 0, 0, 0));
		setSpacing(0);
		setMaxWidth(CHUNK_WIDTH);
		setMaxHeight(CHUNK_HEIGHT);
		setMinWidth(CHUNK_WIDTH);
		setMinHeight(CHUNK_HEIGHT);
		setBorder(new Border(new BorderStroke(Color.BLACK, BorderStrokeStyle.SOLID, CornerRadii.EMPTY,
				new BorderWidths(BORDER_SIZE))));
	}

	/**
	 * Returns the number of threads currently in the chunk.
	 * 
	 * @return The number of threads.
	 */
	public int size() {
		return getChildren().size();
	}

	/**
	 * Adds a threads to the Chunk. Automatically resizes all visuals.
	 * 
	 * Precondition: Requires that the thread is not already in the chunk.
	 * 
	 * @param threadNum number of thread to add.
	 */
	public void add(int threadNum) {
		threadMap.put(threadNum, size());

		Rectangle temp = new Rectangle();
		temp.setStroke(THREAD_BORDER_COLOR);
		temp.setStrokeWidth(THREAD_BORDER_SIZE);

		Color color = Color.RED;

		temp.setFill(color);
		temp.setHeight(CHUNK_HEIGHT - 2 * BORDER_SIZE);

		getChildren().forEach((n) -> {
			Rectangle rect = (Rectangle) n;
			rect.setWidth(Math.floor((double) ((CHUNK_WIDTH - 2. * BORDER_SIZE - 2 * THREAD_BORDER_SIZE * (size() + 1))
					/ (size() + 1))));
		});

		getChildren().add(temp);

		temp.setWidth(1);
		temp.setWidth((CHUNK_WIDTH - 2 * BORDER_SIZE - 2 * THREAD_BORDER_SIZE * size())
				- ((size() - 1) * ((Rectangle) (get(0))).getWidth()));

	}

	/**
	 * Gets the thread at index `index`
	 * 
	 * @param index the index
	 * @return The thread at the index
	 */
	public Node get(int index) {
		return getChildren().get(index);
	}

}
