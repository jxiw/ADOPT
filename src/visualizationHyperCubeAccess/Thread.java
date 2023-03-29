package visualizationHyperCubeAccess;

import javafx.scene.control.Label;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Font;

/**
 * One of the boxes inside of a chunk.
 * 
 * @author Mitchell Gray
 *
 */
public class Thread extends Pane implements Comparable<Thread> {
	private Rectangle rect;
	private Label text;
	private int threadNum;
	private Color color;
	private int size;

	/**
	 * Constructor for Thread
	 * 
	 * @param threadNum The number of the thread.
	 * @param color     The color of the rectangle.
	 * @param size      The number of times this thread has been used.
	 */
	public Thread(int threadNum, Color color, int size) {
		this.color = color;
		this.threadNum = threadNum;
		this.size = size;
		rect = new Rectangle();
		text = new Label();

		text.setFont(new Font(8));
		text.setText(Integer.toString(threadNum));
		text.layoutXProperty().bind(rect.widthProperty().divide(2));
		text.layoutYProperty().bind(rect.heightProperty().divide(2).multiply(-1));

		getChildren().add(rect);
		getChildren().add(text);

		rect.setStroke(Chunk.THREAD_BORDER_COLOR);
		rect.setStrokeWidth(Chunk.THREAD_BORDER_SIZE);
		rect.setFill(color);
		rect.setHeight(Chunk.CHUNK_HEIGHT - 2 * Chunk.BORDER_SIZE);

		this.setOnMouseEntered((e) -> {
			HelloFX.threadText.setText("Thread Number: " + Integer.toString(threadNum));
		});
		this.setOnMouseExited((e) -> {
			HelloFX.threadText.setText("Thread Number: (None) ");
		});
	}

	public void setRectangleWidth(double width) {
		rect.setWidth(width);
	}

	public double getRectangleWidth() {
		return rect.getWidth();
	}

	public int getThreadNum() {
		return threadNum;
	}

	public Color getColor() {
		return color;
	}

	@Override
	public int compareTo(Thread t) {
		return ((Integer) getThreadNum()).compareTo(t.getThreadNum());
	}

	public void incrementSize() {
		size++;
	}

	public int getSize() {
		return size;
	}
}