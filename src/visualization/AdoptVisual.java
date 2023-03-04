package visualization;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AdoptVisual extends JFrame {
    private JPanel contentPane;
    private JButton startAnalysis;
    private JTextField textField;
    private JButton fileButton;
    private JTable table;

    private String[] headerNames = {"Attribute Name", "Table Name", "Select"};
    private DefaultTableModel dtm = new DefaultTableModel(null, headerNames) {
        @Override
        public Class<?> getColumnClass(int col) {
            return getValueAt(0, col).getClass();
        }
    };

    private final static Pattern indexPattern = Pattern.compile("\\((.*?)\\)");
    private final static Pattern detailPattern = Pattern.compile("\'(.*?)\'");
    private String logFilePath;

    public AdoptVisual() {
        setContentPane(contentPane);
//        setModal(true);
        getRootPane().setDefaultButton(startAnalysis);

        table.getTableHeader().setFont(new javax.swing.plaf.FontUIResource("Ayuthaya", Font.PLAIN, 20));
        table.setFont(new javax.swing.plaf.FontUIResource("Ayuthaya", Font.PLAIN, 15));

        table.setModel(dtm);
        startAnalysis.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onOK();
            }
        });

        fileButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                JFileChooser chooser = new JFileChooser();
                int choice = chooser.showOpenDialog(AdoptVisual.this);
                if (choice != JFileChooser.APPROVE_OPTION) return;
                File chosenFile = chooser.getSelectedFile();
                textField.setText(chosenFile.getAbsolutePath());
                try {
                    BufferedReader br = new BufferedReader(new FileReader(chosenFile.getAbsolutePath()));
                    String indexLine = br.readLine();
                    Matcher indexMatcher = indexPattern.matcher(indexLine);
                    int indexNum = 0;
                    ArrayList<String[]> dataInString = new ArrayList<String[]>();
                    while (indexMatcher.find()) {
                        String indexResult = indexMatcher.group(1);
                        System.out.println(indexResult);
                        String[] indexInfos = new String[3];
                        Matcher detailInfoMatcher = detailPattern.matcher(indexResult);
                        for (int i = 0; i < 3; i++) {
                            detailInfoMatcher.find();
                            indexInfos[i] = detailInfoMatcher.group(1);
                        }
                        dataInString.add(indexInfos);
                        indexNum++;
                    }
                    Object[][] data = new Object[indexNum][3];
                    for (int i = 0; i < indexNum; i++) {
                        System.arraycopy(dataInString.get(i), 0, data[i], 0, 2);
                        data[i][2] = false;
                        dtm.addRow(data[i]);
                    }

                    logFilePath = chosenFile.getAbsolutePath();
                    br.close();
                } catch (FileNotFoundException ex) {
                    ex.printStackTrace();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }

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
    }

    private void onOK() {
        // add your code here
        // this.setVisible(false);
        // dispose();

        System.out.println(dtm.getDataVector());
        Vector<Vector> selectDatas = dtm.getDataVector();
        ArrayList<Integer> indexIds = new ArrayList<Integer>();
        ArrayList<String> indexColumns = new ArrayList<String>();
        for (int i = 0; i < selectDatas.size(); i++) {
            Vector selectData = selectDatas.get(i);
            boolean isSelect = (boolean) selectData.get(3);
            if (isSelect) {
                indexIds.add(i);
                indexColumns.add((String) selectData.get(2));
            }
        }

        ArrayList<Integer> lightIds = new ArrayList<Integer>();
        ArrayList<String> lightColumns = new ArrayList<String>();

        Runnable r = () -> {
            VisualRunner runner = new VisualRunner();
            runner.analysisFile(logFilePath);
        };

        new Thread(r).start();
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

}
