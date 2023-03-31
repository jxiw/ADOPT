package totalVisualization;

import javafx.application.Platform;
import javafx.stage.Stage;

import javax.swing.*;
import java.awt.event.*;
import java.io.*;
import javafx.embed.swing.JFXPanel;
import java.util.regex.Pattern;

public class AdoptVisual extends JFrame {
    private JPanel contentPane;
    private JTextField textField;
    private JButton fileButton;
    private JComboBox comboBox;
    private JButton threadButton;
    private JButton treeButton;
    private JButton attributeButton;
    private JButton cubeButton;
    private JTextArea sqlText;

    private final static Pattern indexPattern = Pattern.compile("\\((.*?)\\)");
    private final static Pattern detailPattern = Pattern.compile("\'(.*?)\'");
    private String logFilePath;

    public AdoptVisual() {
        setContentPane(contentPane);
//        setModal(true);
//        getRootPane().setDefaultButton(startAnalysis);

        fileButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                int choice = chooser.showOpenDialog(AdoptVisual.this);
                if (choice != JFileChooser.APPROVE_OPTION) return;
                File chosenFile = chooser.getSelectedFile();
                logFilePath = chosenFile.getAbsolutePath();
                textField.setText(logFilePath);
                HelloFX.globalFileName = logFilePath;
            }
        });

        treeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                popTreeFrame();
            }
        });

        cubeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                popHypercubeFrame();
            }
        });

        attributeButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                popAttributePieFrame();
            }
        });

        threadButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                popThreadPieFrame();
            }
        });

        // call onCancel() when cross is clicked
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                onCancel();
            }
        });

        // call onCancel() on ESCAPE
        contentPane.registerKeyboardAction(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onCancel();
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);

        new JFXPanel();
    }

    private void popTreeFrame() {
        Thread thread = new Thread() {
            public void run() {
                visualization.VisualRunner.main(new String[0]);
            }
        };
        thread.start();
    }

    private void popHypercubeFrame() {
        Platform.runLater(new Runnable() {
            @Override
            public void run() {
                try {
                    (new visualizationHyperCube.HelloFX()).start(new Stage());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    private void popThreadPieFrame() {
        Platform.runLater(new Runnable() {
            public void run() {
                try {
                    (new visualizationThreadPieChart.HelloFX()).start(new Stage());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    private void popAttributePieFrame() {
        Platform.runLater(new Runnable() {
            public void run() {
                try {
                    (new visualizationAttributePieChart.HelloFX()).start(new Stage());
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    private void onCancel() {
        // add your code here if necessary
        dispose();
    }

    public static void main(String[] args) {
        AdoptVisual frame = new AdoptVisual();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(2000, 3000);
        frame.setVisible(true);
    }

    private void createUIComponents() {
        // TODO: place custom component creation code here

    }
}
