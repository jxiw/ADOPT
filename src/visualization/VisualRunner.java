package visualization;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VisualRunner {

    private final static Pattern attributePattern = Pattern.compile("attribute order:\\[(.*)\\]");

    private final static Pattern pattern = Pattern.compile("lftj order index:\\[(.*)\\], reward:(.*)");

    public void analysisFile(String logFilePath) {
        try {
            System.out.println(logFilePath);
            Visualization adoptVisualization = new Visualization();
            BufferedReader br = new BufferedReader(new FileReader(logFilePath));
            String line = br.readLine();
            while (line != null) {
                Matcher n = attributePattern.matcher(line);
                if (n.find()) {
                    String attributesInString = n.group(1);
                    String[] attributes = attributesInString.split("], ");
                    List<String> attributesInList = new ArrayList<>();
                    List<Integer> attributeIdList = new ArrayList<>();
                    for (int i = 0; i < attributes.length; i++) {
                        attributesInList.add(attributes[i].replace("[", "").replace("]", ""));
                        attributeIdList.add(i);
                    }
                    adoptVisualization.init(attributesInList, attributeIdList);
                }
                Matcher m = pattern.matcher(line);
                if (m.find()) {
                    String order = m.group(1);
//                    String attribute = m.group(2);
                    double reward = Double.parseDouble(m.group(2));
                    List<Integer> actions = Arrays.stream(order.split(", "))
                            .map(Integer::parseInt)
                            .collect(Collectors.toList());
                    adoptVisualization.update(actions, reward);
                }
                line = br.readLine();
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        VisualRunner visualRunner = new VisualRunner();
        visualRunner.analysisFile("4_cycle.txt");
    }


}
