package visualizationAttributePieChart;

import util.Pair;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AttributeDataParser {
//	public static final double TOTAL_VOLUME = Math.pow(564882983 - 13 + 1, 3);
	private boolean end = false;
	private FileReader reader;
	private Scanner scan;

	private final static Pattern lftjPattern = Pattern.compile("lftj order index:\\[(.*)\\], reward:(.*), threadId:(.*), select cube:hypercube:(.*), exploreDomain:\\[(.*)\\], (endValues:|finish hypercubes)(.*)");

	private final static Pattern hypercubePattern = Pattern.compile("\\[(.*?)\\]");

	private final static Pattern spacePattern = Pattern.compile("attribute order:\\[(.*)\\], values:\\[(.*)\\]");

	private final static Pattern executionTimePattern = Pattern.compile("thread:(.*), execution time in ms:(.*)");

	private final static int maxThread = 32;

	private static List<String> attributes = new ArrayList<>();

	public static int totalSample = 0;

	/**
	 * Constructor for data parser.
	 * 
	 * @Param filepath File to read from.
	 */
	public AttributeDataParser(String filepath) {
//		bounds = new int[6];

		// Read the file.
		try {
			reader = new FileReader(filepath);
			scan = new Scanner(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
			System.exit(1);
		}

		totalSample = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
			String line;
			while ((line = br.readLine()) != null) {
				Matcher n = spacePattern.matcher(line);
				if (n.find()) {
					String attributeContent = n.group(1);
					Matcher attributeMatch =  hypercubePattern.matcher(attributeContent);
					while (attributeMatch.find()) {
						String attribute = attributeMatch.group(1);
						attributes.add(attribute);
					}
				}
				if (line.startsWith("log lftj order")) {
					totalSample += 1;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the next data for attribute boxes
	 *
	 * @return an int array, the array has a length of 7. Sequential 2 indexes are
	 *         the lower and upper range values respectively for each attribute box.
	 *         [0,1,4,5,9,10] (0,1) are lower and upper for a1, (4,5) for a2, (9,10)
	 *         for a3. The last index, 6, is the Join Order.
	 */
	public Object[] getNext() {
		Object[] returner = new Object[3];

		while (true) {
			String line;

			if (!end)
				line = scan.nextLine();
			else {
				returner[0] = "-1";
				returner[1] = -1.;
				returner[2] = "-1";
				return returner;
			}

			if (line.substring(0).equals("------------")) {
				end = true;
				returner[0] = "-1";
				returner[1] = -1.;
				returner[2] = "-1";
				return returner;

			}

			Matcher n = lftjPattern.matcher(line);
			if (n.find()) {
				String attributeContent = n.group(1);
				String hypercubeRange = n.group(5);
				int threadId = Integer.parseInt(n.group(3));
				returner[2] = attributeContent;
				ArrayList<Pair<Double, Double>> intervals = new ArrayList<>();
				Matcher rangeMatch = hypercubePattern.matcher(hypercubeRange);
				while (rangeMatch.find()) {
					String rangeValue = rangeMatch.group(1);
					String[] rangeArray = rangeValue.split(",");
					intervals.add(new Pair<>(Double.parseDouble(rangeArray[0].trim()), Double.parseDouble(rangeArray[1].trim())));
					int endPointIndex = line.indexOf("endValues");
					if (endPointIndex == -1) {
						returner[1] = getCubeVolume(intervals);
					} else {
						String endPointString = n.group(7);
						endPointString = endPointString.substring(1, endPointString.length() - 1);
						String[] endPoints =  endPointString.split(",");
						List<Double> endPointsDouble = new ArrayList<>();
						for (String endPoint : endPoints) {
							endPointsDouble.add(Double.parseDouble(endPoint));
						}
						returner[1] = getCubeVolume(intervals, endPointsDouble);
					}
				}

				if (threadId < maxThread) {
					break;
				}
			}

		}
		return returner;
	}

	public static double getCubeVolume(List<Pair<Double, Double>> intervals) {
		int dim = intervals.size();
		double vol = 1;
		for (int i = 0; i < dim; i++) {
			double startI = intervals.get(i).getFirst();
			double endI = intervals.get(i).getSecond();
			vol *= (endI - startI + 1);
		}
		return vol;
	}

	public static double getCubeVolume(List<Pair<Double, Double>> intervals, List<Double> endPoints) {
		int dim = intervals.size();
		double allVol = 0;
		for (int i = 0; i < dim; i++) {
			double currentI = endPoints.get(i);
			double startI = intervals.get(i).getFirst();
			double endI = intervals.get(i).getSecond();
			if (currentI > startI) {
				double remindVol = (currentI - startI);
				for (int j = i + 1; j < dim; j++) {
					remindVol *= intervals.get(j).getSecond() - intervals.get(j).getFirst() + 1;
				}
				allVol += remindVol;
			}
		}
		return allVol + 1;
	}

	public static String showAttributeInString() {
		return IntStream.range(0, attributes.size())
				.mapToObj(i -> i + ": [" + attributes.get(i) + "]")
				.collect(Collectors.joining(", "));
	}

	public static void main(String[] args) {
		AttributeDataParser data = new AttributeDataParser("src/visualizationHyperCube/run4_budget.txt");

		int j = 0;
		while (j < 20) {
			try {
				Object[] testNext = data.getNext();
				for (int i = 0; i < testNext.length; i++) {
					System.out.println(testNext[i]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			j++;
		}

	}
}
