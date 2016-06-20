import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Mapper_1_pairs extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);

	private Text triple = new Text();

	/*
	 * Lista contenente i valori visitati, se si incontra un valore visitato
	 * precedentemente si salta affinchè il conteggio non venga ripetuto più di
	 * una volta
	 */
	private ArrayList<String> sameValues = new ArrayList<String>();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("----------------------------------MAPPER1---------------------------------------");

		String[] tokens = value.toString().split("\\s+");
		int size = tokens.length;

		if (tokens.length > 2) {
			for (int i = 0; i < size; i++) {

				if (sameValues.contains(tokens[i]))
					continue;
				else
					sameValues.add(tokens[i]);

				for (int j = i + 1; j < size; j++) {

					if (tokens[j].equals(tokens[i]))
						continue;

					for (int k = j + 1; k < size; k++) {

						if (tokens[k].equals(tokens[i]))
							continue;
						if (tokens[k].equals(tokens[j]))
							continue;

						triple.set(tokens[i] + " " + tokens[j] + " " + tokens[k]); // ABC
						context.write(triple, ONE);
						triple.set(tokens[i] + " " + tokens[k] + " " + tokens[j]); // ACB
						context.write(triple, ONE);
						triple.set(tokens[j] + " " + tokens[i] + " " + tokens[k]); // BAC
						context.write(triple, ONE);
						triple.set(tokens[j] + " " + tokens[k] + " " + tokens[i]); // BCA
						context.write(triple, ONE);
						triple.set(tokens[k] + " " + tokens[i] + " " + tokens[j]); // CAB
						context.write(triple, ONE);
						triple.set(tokens[k] + " " + tokens[j] + " " + tokens[i]); // CBA
						context.write(triple, ONE);
					}
				}
			}
			sameValues.clear();
		}
	}
}
