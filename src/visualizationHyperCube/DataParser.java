package visualizationHyperCube;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Scanner;

public class DataParser {

	private boolean end = false;
	private FileReader reader;
	private Scanner scan;
	private int[] bounds;

	public DataParser() {
		bounds = new int[6];

		// Read the file.
		try {
			reader = new FileReader("src/visualizationHyperCube/run4_budget.txt");
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
	 *         for a3. The last index, 6, is the threadNum.
	 */
	public int[] getNext() {
		int[] returner = new int[7];

		while (true) {
			String line;

			if (!end)
				line = scan.nextLine();
			else
				break;

			if (line.substring(0).equals("------------")) {
				end = true;
				for (int i = 0; i < returner.length; i++) {
					returner[i] = -1;
				}
			}

			if (line.length() >= 7 && line.substring(0, 8).equals("log lftj")) {
				System.out.println(line);
				String[] temp = line.split(":");
				String[] temp2 = temp[5].split(",");

				returner[0] = Integer.parseInt(temp2[6].substring(16));
				returner[1] = Integer.parseInt(temp2[7].substring(0, temp2[7].length() - 1));
				returner[2] = Integer.parseInt(temp2[8].substring(2));
				returner[3] = Integer.parseInt(temp2[9].substring(0, temp2[9].length() - 1));
				returner[4] = Integer.parseInt(temp2[10].substring(2));
				returner[5] = Integer.parseInt(temp2[11].substring(0, temp2[11].length() - 2));
				returner[6] = Integer.parseInt(temp[3].substring(0, temp[3].indexOf(",")));

				for (int i : returner) {
					System.out.println(i);
				}
				if (returner[6] >= 10) {
					continue;
				}
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

	public static void main(String[] args) {
		DataParser data = new DataParser();

		for (int i : data.getBounds()) {
			System.out.println(i);
		}

	}
}
