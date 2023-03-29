package visualization;

import java.awt.Color;
import java.awt.Font;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.swing.JLabel;
import javax.swing.JTextPane;
import javax.swing.text.BadLocationException;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.graphicGraph.stylesheet.StyleConstants;
import org.graphstream.ui.layout.LayoutRunner;
import org.graphstream.ui.spriteManager.Sprite;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.Viewer;

/**
 * Creates and manages a visualization of the query.
 */
public class Visualization {
	private int count = 0;
	private SingleGraph graph;
	private Viewer viewer;
	private ViewPanel view;
	private SpriteManager spriteManager;
	private TreeLayout layout;
	private JLabel counterLabel;
	private JTextPane textPane;
	private int iterationCounter;
	private Map<String, Integer> numVisits;
	private Map<String, Double> maxReward;
	private Map<String, Double> rewardSum;
	private Map<String, Integer> iteration;
//    private ViewerPipe pipeIn;

	private Color colors[] = { Color.RED, Color.YELLOW, Color.GREEN, Color.CYAN, Color.BLUE, Color.MAGENTA, Color.GRAY,
			Color.PINK, Color.magenta, Color.orange };

//    private String indices[] = {"(C_W_ID,C_D_ID,C_LAST,C_FIRST)", "(C_W_ID,C_D_ID,C_LAST)", "(C_W_ID,C_D_ID, C_ID)", "(C_W_ID,C_D_ID)", "(C_W_ID)", "(C_D_ID)"};

//    private String indices[] = {"code block 0", "code block 1", "code block 2", "code block 3", "code block 4", "code block 5", "code block 6"};

	private List<Integer> lightIds;

	private List<String> lightColumns;

	private final String stylesheet = "" + "graph {" + " padding: 0%;" + "}" + "" + "sprite.counter {"
			+ " fill-mode: none;" + " text-size: 14%;" + "} " + "" + "sprite.join { " + " shape: flow; "
			+ " size: 0.04%;" + " z-index: 0; " + " sprite-orientation: from;" + " fill-color: White;" + "} "
			+ "sprite.visible { " + " shape: flow; " + " size: 0.04%;" + " z-index: 0; " + " sprite-orientation: from;"
			+ " fill-color: LIGHTGRAY;" + "} " + "" + "node {" + " size: 30px;" +
			// " fill-color: white;" +
			// " text-color: white;" +
			// " text-style: bold;" +
			// " text-padding: 2px;" +
			" text-size: 25%;" +
			// " text-background-mode: rounded-box;" +
			// " text-background-color: rgb(35, 47, 62);" +
			" fill-mode : dyn-plain;" + " size-mode: dyn-size;" + "}" + "" + "edge {" + "  arrow-shape: none;"
			+ "  text-size: 1%;" + "} " + "" + "node.root {" + " size: 30px;" +
			// " fill-color: white;" +
			// " text-color: white;" +
			// " text-style: bold;" +
			" text-alignment: above;" + " text-size: 25%;" +
			// " text-background-mode: rounded-box;" +
			// " text-background-color: rgb(35, 47, 62);" +
			" fill-mode : dyn-plain;" + " size-mode: dyn-size;" + "}";

	/**
	 * Initializes the state/data structures of the query visualizer
	 */
	public void init(List<String> lightColumns, List<Integer> lightIds) {
		// Prevent the Graphstream Layout manager from logging to std out
		Logger.getLogger(LayoutRunner.class.getSimpleName()).setUseParentHandlers(false);

		this.lightColumns = lightColumns;
		this.lightIds = lightIds;

		// Initialize viewer
		System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer");
		graph = new SingleGraph("");
		viewer = graph.display(false);
		viewer.setCloseFramePolicy(Viewer.CloseFramePolicy.CLOSE_VIEWER);

		viewer.disableAutoLayout();
		view = viewer.getDefaultView();
		view.setLayout(null);
		view.getCamera().setViewPercent(1.1);
		view.getCamera().setViewCenter(0, -2, 0);

		// Setup layout
		layout = new TreeLayout(graph);

		iterationCounter = 0;
		counterLabel = new JLabel("Number of Samples: " + iterationCounter);
		counterLabel.setFont(new Font("Serif", Font.PLAIN, 32));
		counterLabel.setBounds(25, 0, 500, 50);
		view.add(counterLabel);

		textPane = new JTextPane();
		textPane.setFont(new Font("Serif", Font.PLAIN, 48));
		textPane.setBounds(25, 50, 300, 200);
		for (int i = 0; i < lightColumns.size(); i++) {
			String index = lightColumns.get(i);
			print(textPane, "⬤", colors[i], Color.WHITE);
			print(textPane, index + "\n", Color.BLACK, Color.WHITE);
		}
		textPane.setEditable(false);
		view.add(textPane);
		spriteManager = new SpriteManager(graph);
		graph.setAttribute("ui.stylesheet", stylesheet);
		graph.setAttribute("ui.antialias");
		graph.setAttribute("ui.quality");

		// Root node
//        addNode("root").addAttribute("ui.label", "Code Reorder Search Tree");
		Node root = addNode("root");
		root.addAttribute("ui.label", "Parameter Tree");
		root.addAttribute("ui.class", "root");
		root.addAttribute("ui.basecolor", Color.GRAY);
		root.addAttribute("ui.color", Color.GRAY);

		// Data structures for max/average reward for join order
		numVisits = new HashMap<>();
		maxReward = new HashMap<>();
		rewardSum = new HashMap<>();
		iteration = new HashMap<>();

	}

	private void print(JTextPane textPane, String msg, Color foreground, Color background) {
		MutableAttributeSet attributes = new SimpleAttributeSet(textPane.getInputAttributes());
		javax.swing.text.StyleConstants.setForeground(attributes, foreground);
		javax.swing.text.StyleConstants.setBackground(attributes, background);

		try {
			textPane.setFont(new javax.swing.plaf.FontUIResource("Ayuthaya", Font.PLAIN, 28));
			textPane.getStyledDocument().insertString(textPane.getDocument().getLength(), msg, attributes);
		} catch (BadLocationException ignored) {
		}
	}

	/**
	 * Update the visualization for the current iteration.
	 *
	 * @param actions sampled action
	 * @param reward  the reward received
	 */
	public void update(java.util.List<Integer> actions, double reward) {

		incrementCounter();
		for (Sprite sprite : spriteManager.sprites()) {
			if (sprite.hasAttribute("progress")) {
				sprite.setAttribute("ui.hide");
			}
		}

		if (createNodesSpriteIfNotPresent(actions)) {
			layout.compute();
		}

		count++;

		for (Node node : graph.getNodeSet()) {
			changeNodeTransparency(node.getId());
		}

		String currentJoinNode = "";
		String previous = "root";
		double currentReward = 0;
		for (int orderIdx = 0; orderIdx < actions.size(); orderIdx++) {
			int action = actions.get(orderIdx);
			int findPos = -1;
			for (int currentPos = 0; currentPos < lightIds.size(); currentPos++) {
				if (lightIds.get(currentPos) == action) {
					findPos = currentPos;
					break;
				}
			}
			if (findPos > -1) {
				currentReward = reward;
				currentJoinNode += action + "";
				System.out.println("S#" + previous + "--" + currentJoinNode);
				String spriteId = "S#" + previous + "--" + currentJoinNode;
				Sprite sprite = spriteManager.getSprite(spriteId);
				sprite.removeAttribute("ui.hide");
				numVisits.put(currentJoinNode,
						1 + (numVisits.containsKey(currentJoinNode) ? numVisits.get(currentJoinNode) : 0));
				changeNodeSize(currentJoinNode);
				previous = currentJoinNode;
				updateRewardLabels(currentJoinNode, currentReward);
			}
		}

		frameTimeDelay();
//        pipeIn.pump();
	}

	/**
	 * Add a node to the graph
	 *
	 * @param id node id
	 * @return Node object
	 */
	private Node addNode(String id) {
		Node node = graph.addNode(id);
		layout.nodeAdded(id);
		return node;
	}

	/**
	 * Add an edge to the graph
	 *
	 * @param edgeId   edge id
	 * @param fromId   node id of the source
	 * @param toId     node id of the target
	 * @param directed whether the edge is directed
	 * @return Edge object
	 */
	public Edge addEdge(String edgeId, String fromId, String toId, boolean directed) {
		Edge edge = graph.addEdge(edgeId, fromId, toId, directed);
		layout.edgeAdded(edgeId, fromId, toId, directed);
		return edge;
	}

	/**
	 * Increment the iteration counter and update the label.
	 */
	private void incrementCounter() {
		iterationCounter++;
		counterLabel.setText("Number of Samples: " + iterationCounter);
	}

	/**
	 * Creates the nodes/edges/join progress bars and reward labels for a given join
	 * order if it doesn't exist already
	 *
	 * @param actions the actions
	 * @return whether or not the graph was modified
	 */
	public boolean createNodesSpriteIfNotPresent(List<Integer> actions) {
		String currentJoinNode = "";
		String previous = "root";
		boolean globalModified = false;
		for (int action : actions) {
			boolean modified = false;
			int findPos = -1;
			for (int currentPos = 0; currentPos < lightIds.size(); currentPos++) {
				if (lightIds.get(currentPos) == action) {
					findPos = currentPos;
					break;
				}
			}

			if (findPos > -1) {
				Color color = colors[findPos];
				String actionName = action + "";
				currentJoinNode += actionName;
				if (graph.getNode(currentJoinNode) == null) {
					Node newNode = addNode(currentJoinNode);
					newNode.addAttribute("ui.label", actionName);
					newNode.addAttribute("ui.basecolor", color);
					Edge edge = addEdge(previous + "--" + currentJoinNode, previous, currentJoinNode, true);

					System.out.println("S#" + previous + "--" + currentJoinNode);
					Sprite sprite = spriteManager.addSprite("S#" + previous + "--" + currentJoinNode);
					sprite.addAttribute("ui.class", "join");// join
					sprite.addAttribute("ui.color", new Color(255, 0, 0, 0));// join
					sprite.addAttribute("progress");
					sprite.attachToEdge(edge.getId());
					sprite.setPosition(0);
					modified = true;
				}
				previous = currentJoinNode;
			}

			if (modified) {
				Sprite maxSprite = spriteManager.addSprite("SM#" + currentJoinNode);
				maxSprite.addAttribute("ui.class", "counter");
				maxSprite.attachToNode(currentJoinNode);
				maxSprite.setPosition(StyleConstants.Units.PX, 22, 138, -90);

				Sprite avgSprite = spriteManager.addSprite("SA#" + currentJoinNode);
				avgSprite.addAttribute("ui.class", "counter");
				avgSprite.attachToNode(currentJoinNode);
				avgSprite.setPosition(StyleConstants.Units.PX, 40, 250, -90);
				globalModified = true;

				Sprite iterationSprite = spriteManager.addSprite("SM#" + currentJoinNode + "SA#" + currentJoinNode);
				iterationSprite.addAttribute("ui.class", "counter");
				iterationSprite.attachToNode(currentJoinNode);
				iterationSprite.setPosition(StyleConstants.Units.PX, 58, 362, -90);
			}
		}

		return globalModified;
	}

	private void frameTimeDelay() {
//        sleep(10);
		if (iterationCounter < 5) {
			sleep(800);
		} else if (iterationCounter < 10) {
			sleep(500);
		}
		sleep(10);
//        else if (iterationCounter < 15) {
//            sleep(500);
//        } else if (iterationCounter < 50) {
//            sleep(125);
//        } else if (iterationCounter < 150) {
//            sleep(35);
//        } else if (iterationCounter < 500) {
//            sleep(10);
//        }
	}

	/**
	 * Color the given node depending on the number of visits
	 *
	 * @param node node id
	 */
	private void changeNodeSize(String node) {
		int num = Math.min(numVisits.get(node), 100);
		double factor = Math.log10(100);
//        int upper = 64;
//        int lower = 34;
		int upper = 60;
		int lower = 20;
		long gb = Math.round((Math.log10(num) / factor) * (upper - lower)) + lower;
//        String color = "rgb(255, " + gb + ", " + gb + ")";
//        graph.getNode(node)
//                .addAttribute("ui.style", "fill-color: " + color + ";");

		graph.getNode(node).addAttribute("ui.size", gb);

	}

	/**
	 * Change the transparency based on the current iteration and last iteration
	 * node was touched.
	 * 
	 * @param node nodeId
	 */
	private void changeNodeTransparency(String node) {

		String substring = node.substring(0, node.length() - 1).isEmpty() ? "root"
				: node.substring(0, node.length() - 1);

		Sprite joinSprite = spriteManager.getSprite("S#" + substring + "--" + node);

		if (joinSprite != null) {

			joinSprite.setAttribute("ui.class", "join");
		}

		Color baseColor = (Color) graph.getNode(node).getAttribute("ui.basecolor") == null ? Color.LIGHT_GRAY
				: (Color) graph.getNode(node).getAttribute("ui.basecolor");
		graph.getNode(node).setAttribute("ui.color", baseColor);
//		graph.getNode(node).addAttribute("ui.color", );
		Color nodeColor = graph.getNode(node).getAttribute("ui.color");
		int red = nodeColor.getRed();
		int green = nodeColor.getGreen();
		int blue = nodeColor.getBlue();

		int totalIterations = count;
		int iterations = iteration.getOrDefault(node, count);

		graph.getNode(node).addAttribute("ui.color", new Color(red, green, blue,
				(int) (((double) 255 - (Math.min(((double) totalIterations - iterations) / 100, 10)) * 25.5))));
	}

	/**
	 * Update reward labels for a given leaf node
	 *
	 * @param currentJoinNode the ID of the leaf node
	 * @param reward          the reward for this sample
	 */
	private void updateRewardLabels(String currentJoinNode, double reward) {
		if (!maxReward.containsKey(currentJoinNode)) {
			maxReward.put(currentJoinNode, reward);
		} else {
			maxReward.put(currentJoinNode, Math.max(reward, maxReward.get(currentJoinNode)));
		}

		if (!rewardSum.containsKey(currentJoinNode)) {
			rewardSum.put(currentJoinNode, reward);
		} else {
			rewardSum.put(currentJoinNode, reward + maxReward.get(currentJoinNode));
		}

		if (!iteration.containsKey(currentJoinNode)) {
			iteration.put(currentJoinNode, count);
		} else {
			iteration.put(currentJoinNode, count);
		}

		graph.getNode(currentJoinNode).addAttribute("ui.color", Color.LIGHT_GRAY);

		Sprite rewardSprite = spriteManager.getSprite("SM#" + currentJoinNode);
		rewardSprite.setAttribute("ui.label", "Max: " + String.format("%6.2e", maxReward.get(currentJoinNode)));

		Sprite averageRewardSprite = spriteManager.getSprite("SA#" + currentJoinNode);
		double average = rewardSum.get(currentJoinNode) / numVisits.get(currentJoinNode);
		averageRewardSprite.setAttribute("ui.label", "Average: " + String.format("%6.2e", average));

		Sprite iterationSprite = spriteManager.getSprite("SM#" + currentJoinNode + "SA#" + currentJoinNode);
		iterationSprite.setAttribute("ui.label", "Iteration: " + count);

		String substring = currentJoinNode.substring(0, currentJoinNode.length() - 1).isEmpty() ? "root"
				: currentJoinNode.substring(0, currentJoinNode.length() - 1);

		Sprite joinSprite = spriteManager.getSprite("S#" + substring + "--" + currentJoinNode);

		joinSprite.setAttribute("ui.class", "visible");
	}

	private void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}
}
