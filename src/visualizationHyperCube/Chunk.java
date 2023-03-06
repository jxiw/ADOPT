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
import javafx.stage.Stage;

/**
 * Represents an aggregation of rows.
 * 
 * @author Mitchell Gray
 *
 */
public class Chunk extends HBox {
	public static final double BORDER_SIZE = 1.;
	public static final Color BORDER_COLOR = Color.BLACK;
	public static final double CHUNK_HEIGHT = 10.;
	public static final double CHUNK_WIDTH = 420.;
	public static final double THREAD_BORDER_SIZE = 1.;
	public static final Color THREAD_BORDER_COLOR = Color.BLACK;

	private int lowerRange;
	private int upperRange;

	/**
	 * Keys are threadNums and Values are position in getChildren() List.
	 */
	private HashMap<Integer, Thread> threadMap;

	/**
	 * @param stage      The primary stage of the application.
	 * @param lowerRange The lower numerical value of the range of the attribute.
	 * @param upperRange The higher numerical value of the range of the attribute.
	 */
	public Chunk(Stage stage, int lowerRange, int upperRange) {
		this.lowerRange = lowerRange;
		this.upperRange = upperRange;
		threadMap = new HashMap<Integer, Thread>();
		setPadding(new Insets(0, 0, 0, 0));
		HBox.setMargin(this, new Insets(0, 0, 0, 0));
		setSpacing(0);
		setMaxWidth(CHUNK_WIDTH);
		setMaxHeight(CHUNK_HEIGHT);
		setMinWidth(CHUNK_WIDTH);
		setMinHeight(CHUNK_HEIGHT);
		setBorder(new Border(new BorderStroke(BORDER_COLOR, BorderStrokeStyle.SOLID, CornerRadii.EMPTY,
				new BorderWidths(BORDER_SIZE))));

		this.setOnMouseEntered((e) -> {
			HelloFX.chunkText.setText("Chunk Range: " + "(" + lowerRange + "," + upperRange + ")" + " ");
		});
		this.setOnMouseExited((e) -> {
			HelloFX.chunkText.setText("Chunk Range: (None) ");
		});
	}

	/**
	 * Returns the number of threads currently in the chunk.
	 * 
	 * @return The number of threads.
	 */
	public int size() {
		return getChildren().size();
	}

	public int lowerRange() {
		return lowerRange;
	}

	public int upperRange() {
		return upperRange;
	}

	/**
	 * Adds a threads to the Chunk. Automatically resizes all visuals. Can't add
	 * Threads already in the Chunk.
	 * 
	 * @param threadNum number of thread to add.
	 */
	public void add(int threadNum) {
		if (!threadMap.containsKey(threadNum)) {
			Color color = HelloFX.getColor(threadNum);
			Thread thread = new Thread(threadNum, color);
			threadMap.put(threadNum, thread);

			getChildren().add(thread);

			resize();
			sort();
			resizeLastThread();
		}
	}

	/**
	 * Removes a thread from Chunk. Automatically resizes all visuals.
	 * 
	 * @param threadNum
	 */
	public void remove(int threadNum) {
		getChildren().remove(threadMap.get((Integer) threadNum));
		threadMap.remove((Integer) threadNum);

		resize();
		sort();

		if (size() > 0) {
			resizeLastThread();
		}

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

	/**
	 * Determines whether or not the Chunk already contains the thread.
	 * 
	 * @param t The Thread number to look for.
	 * @return Whether or not t was found.
	 */
	public boolean contains(int t) {
		return threadMap.containsKey(t);
	}

	/**
	 * Resizes all Thread rectangles to evenly distribute across the Chunk.
	 */
	private void resize() {
		getChildren().forEach((n) -> {
			Thread temp = (Thread) n;
			temp.getRectangle().setWidth(Math
					.floor((double) ((CHUNK_WIDTH - 2. * BORDER_SIZE - 2 * THREAD_BORDER_SIZE * (size())) / (size()))));
		});
	}

	/**
	 * Resizes the last Thread to take any remaining space to remove white space.
	 */
	private void resizeLastThread() {
		((Thread) getChildren().get(size() - 1)).getRectangle().setWidth(CHUNK_WIDTH - 2 * BORDER_SIZE
				- 1 * THREAD_BORDER_SIZE * (size()) - ((size() - 1) * ((Thread) (get(0))).getRectangle().getWidth()));
	}

	/**
	 * Sort all threads in the chunk to make sure colors are consistent.
	 */
	private void sort() {
		for (int i = 0; i < getChildren().size(); i++) {
			for (int j = getChildren().size() - 1; j > i; j--) {
				if (((Thread) getChildren().get(i)).compareTo((Thread) getChildren().get(j)) > 0) {
					Thread tmp = (Thread) getChildren().get(i);
					Thread tmp2 = (Thread) getChildren().get(j);

					getChildren().set(i, new Thread(-1, tmp.getColor()));
					getChildren().set(j, new Thread(-1, tmp2.getColor()));
					getChildren().set(i, tmp2);
					getChildren().set(j, tmp);
				}
			}
		}
	}

}
