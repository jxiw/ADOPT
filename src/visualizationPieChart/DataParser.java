package visualizationPieChart;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import util.Pair;

public class DataParser {
	public static final double TOTAL_VOLUME = Math.pow(564882983 - 13 + 1, 3);
	private boolean end = false;
	private FileReader reader;
	private Scanner scan;
	private int[] bounds;

	/**
	 * Constructor for data parser.
	 * 
	 * @Param filepath File to read from.
	 */
	public DataParser(String filepath) {
		bounds = new int[6];

		// Read the file.
		try {
			reader = new FileReader(filepath);
			scan = new Scanner(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.out);
			System.exit(1);
		}

		while (true) {
			String line = scan.nextLine();

			if (line.length() >= 15 && line.substring(0, 15).equals("attribute order")) {
				String[] temp = line.split(":");
				String[] temp2 = temp[2].split(",");

				bounds[0] = Integer.parseInt(temp2[0].substring(3));
				bounds[1] = Integer.parseInt(temp2[1].substring(1, temp2[1].length() - 1));
				bounds[2] = Integer.parseInt(temp2[2].substring(2));
				bounds[3] = Integer.parseInt(temp2[3].substring(1, temp2[3].length() - 1));
				bounds[4] = Integer.parseInt(temp2[4].substring(2));
				bounds[5] = Integer.parseInt(temp2[5].substring(1, temp2[5].length() - 2));

				break;
			}

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
		Object[] returner = new Object[2];

		while (true) {
			String line;

			if (!end)
				line = scan.nextLine();
			else {
				returner[0] = "-1";
				returner[1] = -1.;
				return returner;
			}

			if (line.substring(0).equals("------------")) {
				end = true;
				returner[0] = "-1";
				returner[1] = -1.;
				return returner;

			}

			if (line.length() >= 7 && line.substring(0, 8).equals("log lftj")) {
				String[] temp = line.split(":");
				String[] temp2 = temp[5].split(",");

				ArrayList<Pair<Double, Double>> intervals = new ArrayList<>();
				Pair<Double, Double> pairOne = new Pair<>(Double.parseDouble(temp2[6].substring(16)),
						Double.parseDouble(temp2[7].substring(0, temp2[7].length() - 1)));
				Pair<Double, Double> pairTwo = new Pair<>(Double.parseDouble(temp2[8].substring(2)),
						Double.parseDouble(temp2[9].substring(0, temp2[9].length() - 1)));
				Pair<Double, Double> pairThree = new Pair<>(Double.parseDouble(temp2[10].substring(2)),
						Double.parseDouble(temp2[11].substring(0, temp2[11].length() - 2)));

				intervals.add(pairOne);
				intervals.add(pairTwo);
				intervals.add(pairThree);

				int commaIndex = temp[1].indexOf(",", 6);
				returner[0] = temp[1].substring(1, commaIndex - 1);

				int endPointIndex = line.indexOf("endValues");
				if (endPointIndex == -1) {
					returner[1] = (pairOne.getSecond() - pairOne.getFirst() + 1)
							* (pairTwo.getSecond() - pairTwo.getFirst() + 1)
							* (pairThree.getSecond() - pairThree.getFirst() + 1);

					return returner;
				}

				ArrayList<Double> endPoints = new ArrayList<>();

				String[] endPointsArray = line.substring(endPointIndex + 11, line.length() - 1).split(", ");

				for (String s : endPointsArray) {
					endPoints.add(Double.parseDouble(s));
				}

				double volume = getCubeVolume(intervals, endPoints);
				returner[1] = volume;
				break;
			}

		}

		return returner;
	}

	/**
	 * Get the bounds for attribute boxes
	 * 
	 * @return an int array, the array has a length of 6. Sequential 2 indexes are
	 *         the lower and upper bound respectively for each attribute box.
	 *         [0,1,4,5,9,10] (0,1) are lower and upper for a1, (4,5) for a2, (9,10)
	 *         for a3.
	 */
	public int[] getBounds() {
		return bounds;
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

	public static void main(String[] args) {
		DataParser data = new DataParser("src/visualizationHyperCube/run4_budget.txt");

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
