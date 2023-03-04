package visualization;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.ProxyPipe;
import org.graphstream.ui.layout.HierarchicalLayout;
import org.graphstream.ui.layout.Layout;
import org.graphstream.ui.spriteManager.SpriteManager;
import org.graphstream.ui.swingViewer.ViewPanel;
import org.graphstream.ui.view.View;
import org.graphstream.ui.view.Viewer;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;
import java.awt.*;
import java.util.HashSet;

public class Component {

    private static String indices1[] = {"(C_W_ID,C_D_ID,C_LAST,C_FIRST)", "(C_W_ID,C_D_ID,C_LAST)", "(C_W_ID,C_D_ID,C_ID)", "(C_W_ID,C_D_ID)", "(C_W_ID)", "(C_D_ID)"};

    private static String indices2[] = {"(NO_D_ID,NO_W_ID,NO_O_ID)", "(NO_D_ID,NO_W_ID)", "(NO_D_ID)"};

    private static String indices3[] = {"(O_W_ID,O_D_ID,O_C_ID,O_ID)", "(O_W_ID,O_D_ID,O_ID)", "(O_W_ID,O_D_ID)", "(D_W_ID,D_ID)"};

    private static String indices4[] = {"(OL_O_ID,OL_D_ID,OL_W_ID)", "(OL_NUMBER,OL_QUANTITY)"};

    private static String indices5[] = {"(S_W_ID,S_I_ID,S_YTD)", "(S_W_ID,S_I_ID,S_QUANTITY)"};

    private static String indices6[] = {"(D_W_ID,O_D_ID)"};

    private static String tx1[] = {"(NO_D_ID,NO_W_ID,NO_O_ID)", "(O_W_ID,O_D_ID,O_ID)", "(C_W_ID,C_D_ID,C_ID)", "(OL_O_ID,OL_D_ID,OL_W_ID)"};

    private static String tx2[] = {"(C_W_ID,C_D_ID,C_ID)", "(D_W_ID,O_D_ID)", "(S_W_ID,S_I_ID,S_YTD)"};

    private static String tx3[] = {"(C_W_ID,C_D_ID,C_LAST,C_FIRST)", "(C_W_ID,C_D_ID,C_ID)", "(OL_O_ID,OL_D_ID,OL_W_ID)", "(O_W_ID,O_D_ID,O_C_ID,O_ID)"};

    private static String tx4[] = {"(D_W_ID,D_ID)", "(O_W_ID,O_D_ID)", "(C_W_ID,C_D_ID,C_ID)", "(C_W_ID,C_D_ID,C_LAST,C_FIRST)"};

    private static String tx5[] = {"(D_W_ID,D_ID)", "(OL_O_ID,OL_D_ID,OL_W_ID)", "(S_W_ID,S_I_ID,S_QUANTITY)"};

    //    private static String tables[] = {"Customer", "NewOrder", "Order", "OrderLine", "Stock", "Distinct"};
    private static String tables[] = {"Component 1", "Component 2", "Component 3", "Component 4", "Component 5", "Component 6"};

    private static String edgeType[] = {"Same Table", "Same Transaction"};

    private static SingleGraph graph;

    private static HierarchicalLayout layout;

    private static HashSet<String> edgeSet = new HashSet<>();

    private static Color colors[] = {Color.PINK, Color.YELLOW, Color.GREEN, Color.CYAN, Color.BLUE, Color.MAGENTA, Color.GRAY};

    private static Color edgeColors[] = {Color.BLACK, Color.BLACK};

    private static final String stylesheet = "" +
            "graph {" +
            " padding: 60px;" +
            "}" +
            "" +
//        "sprite.counter {" +
//        " fill-mode: none;" +
//        " text-size: 18px;" +
//        "} " +
            "" +
//        "sprite.join { " +
//        " shape: flow; " +
//        " size: 5px;" +
//        " z-index: 0; " +
//        " sprite-orientation: from;" +
//        " fill-color: green;" +
//        "} " +
            "" +
            "node {" +
            " size: 45px;" +
            " text-size: 20px;" +
            " fill-mode : dyn-plain;" +
            " size-mode: dyn-size;" +
            "}" +
            "" +
            "edge {" +
//        "  arrow-shape: none;" +
//        "  text-size: 30px;" +
            " fill-mode : dyn-plain;" +
            "}";

    /**
     * Add a node to the graph
     *
     * @param id node id
     * @return Node object
     */
    private static Node addNode(String id) {
        Node node = graph.addNode(id);
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
    public static Edge addEdge(String edgeId, String fromId, String toId,
                               boolean directed) {
        Edge edge = graph.addEdge(edgeId, fromId, toId, directed);
        return edge;
    }

    private static void print(JTextPane textPane, String msg, Color foreground, Color background) {
        MutableAttributeSet attributes = new SimpleAttributeSet(textPane.getInputAttributes());
        javax.swing.text.StyleConstants.setForeground(attributes, foreground);
        javax.swing.text.StyleConstants.setBackground(attributes, background);

        try {
            textPane.getStyledDocument().insertString(textPane.getDocument().getLength(), msg, attributes);
        } catch (BadLocationException ignored) {
        }
    }

    public static void main(String args[]) throws InterruptedException {

        int[] a = {6, 5, 8, 7, 7, 8, 6, 6, 6, 5, 7, 8, 8, 8, 7, 7, 7, 7, 7, 7, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        System.out.println(a.length);
        int dis = 5;
        int[] sum = new int[a.length / dis];
        int j = 0;
        for (int i = 0; i < a.length; i++) {
            sum[j] = sum[j] + a[i];
            if (i % dis == dis - 1) {
                j++;
            }
        }
        for (int i = 0; i < sum.length; i++) {
            System.out.print("(" + i + "," + sum[i] + ")");
        }

        graph = new SingleGraph("Configuration Components");
        graph.setAttribute("ui.stylesheet", stylesheet);
        graph.setAttribute("ui.antialias");
        graph.setAttribute("ui.quality");
        Viewer viewer = graph.display(true);
        ViewPanel view = viewer.getDefaultView();
        view.setLayout(null);

//        // Create a pipe coming from the viewer ...
//        ProxyPipe pipe = viewer.newViewerPipe();
//        // ... and connect it to the graph
//        pipe.addAttributeSink(graph);

        JTextPane textPane = new JTextPane();
        textPane.setBounds(25, 50, 120, 120);
        for (int i = 0; i < tables.length; i++) {
            String index = tables[i];
            print(textPane, "⬤", colors[i], Color.WHITE);
            print(textPane, index + "\n", Color.BLACK, Color.WHITE);
        }
        textPane.setEditable(false);

        view.add(textPane);
        viewer.setCloseFramePolicy(Viewer.CloseFramePolicy.CLOSE_VIEWER);

        for (String index : indices1) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[0]);
        }

        for (String index : indices2) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[1]);
        }

        for (String index : indices3) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[2]);
        }

        for (String index : indices4) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[3]);
        }

        for (String index : indices5) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[4]);
        }

        for (String index : indices6) {
            Node root = addNode(index);
            root.addAttribute("ui.label", index);
            root.addAttribute("ui.color", colors[4]);
        }

        createConnectEdge(7, tx1, edgeColors[1]);
        createConnectEdge(8, tx2, edgeColors[1]);
        createConnectEdge(9, tx3, edgeColors[1]);
        createConnectEdge(10, tx4, edgeColors[1]);
        createConnectEdge(11, tx5, edgeColors[1]);

        createConnectEdge(1, indices1, edgeColors[0]);
        createConnectEdge(2, indices2, edgeColors[0]);
        createConnectEdge(3, indices3, edgeColors[0]);
        createConnectEdge(4, indices4, edgeColors[0]);
        createConnectEdge(5, indices5, edgeColors[0]);
        createConnectEdge(6, indices6, edgeColors[0]);

//        while (true) {
//            // a small delay, avoids full CPU load
//            Thread.sleep(100);
//            // consume the events stored in the buffer, if any
//            pipe.pump();
//
//            // in the development version the previous two instructions can be replaced by
//            // pipe.blockingPump();
//
//        }
    }

    static void createConnectEdge(int id, String indices[], Color color) {
        for (int i = 1; i < indices.length; i++) {
            for (int j = 0; j < i; j++) {
                String idx1 = indices[i];
                String idx2 = indices[j];
                String idx = (idx1.compareTo(idx2) > 0) ? (idx2 + idx1) : (idx1 + idx2);
                if (!edgeSet.contains(idx)) {
                    Edge edge = addEdge(id + idx1 + idx2, idx1, idx2, false);
                    edge.addAttribute("ui.color", color);
                    edgeSet.add(idx);
                }
            }
        }
    }
}
