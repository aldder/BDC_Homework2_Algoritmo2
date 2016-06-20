import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by Michael Oertel and Aldo D'Eramo on 03/06/16.
 */

public class Reducer_1_stripes extends Reducer<Text, MapWritable, Text, IntWritable> {
    
	private MapWritable finalStripe = new MapWritable();

	private Text triple = new Text();
	
    @Override
    protected void reduce(Text word, Iterable<MapWritable> stripes, Context context) throws IOException, InterruptedException {
        //System.out.println("----------------------------------REDUCER1---------------------------------------");

    	/*
		 * Crea la stripe finale, unendo tutte quelle ricevute per una
		 * determinata chiave
		 */
		for (MapWritable stripe : stripes) {
			mergeStripes(stripe);
		}

		/*
		 * Scorre la stripe finale contenente
		 * <A;(B,count(AB),(C,count(AC),...,(*,count(A))> ed emette coppie
		 * <(B,A),count(BA)>...<(*,A),count(A)>
		 */
		for (Writable key : finalStripe.keySet()) {
			triple.set(key.toString()+" "+word);
			//pair.setWord(key.toString());
			//pair.setNeighbor(word);
			context.write(triple, (IntWritable) finalStripe.get(key));
		}
		finalStripe.clear();
    }
    
	private void mergeStripes(MapWritable stripe) {
		for (Writable key : stripe.keySet()) {
			IntWritable stripeCount = (IntWritable) stripe.get(key);
			if (finalStripe.containsKey(key)) {
				IntWritable finalCount = (IntWritable) finalStripe.get(key);
				finalCount.set(finalCount.get() + stripeCount.get());
				((IntWritable) finalStripe.get(key)).set(finalCount.get());
			} else {
				finalStripe.put(key, stripeCount);
			}
		}
	}
}
