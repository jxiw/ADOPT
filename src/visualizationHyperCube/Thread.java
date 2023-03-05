package visualizationHyperCube;

import javafx.scene.control.Label;
import javafx.scene.layout.Pane;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;

public class Thread extends Pane implements Comparable<Thread> {
	private Rectangle rect;
	private Label text;
	private int threadNum;
	private Color color;

	public Thread(int threadNum, Color color) {
		this.color = color;
		this.threadNum = threadNum;
		rect = new Rectangle();
		text = new Label();

		text.setText(Integer.toString(threadNum));

		getChildren().add(rect);
		getChildren().add(text);
		rect.setStroke(Chunk.THREAD_BORDER_COLOR);
		rect.setStrokeWidth(Chunk.THREAD_BORDER_SIZE);
		rect.setFill(color);
		rect.setHeight(Chunk.CHUNK_HEIGHT - 2 * Chunk.BORDER_SIZE);
	}

	public Rectangle getRectangle() {
		return rect;
	}

	public void setWidth(double width) {
		rect.setWidth(width);
	}

	public int getThreadNum() {
		return threadNum;
	}

	@Override
	public int compareTo(Thread t) {
		return ((Integer) getThreadNum()).compareTo(t.getThreadNum());
	}

	public Color getColor() {
		return color;
	}
}
