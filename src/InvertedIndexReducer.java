/*=============================================================================
|   Assignment:  Individual assignment: Programming - 3
|       Author:  Sampath Kumar Gunasekaran(sgunase2@uncc.edu)
|       Grader:  Walid Shalaby
|
|       Course:  ITCS 6190
|   Instructor:  Srinivas Akella
|     Due Date:  March 23 at 11:59PM
|
|     Language:  Java 
|     Version :  1.8.0_101
|                
| Deficiencies:  No logical errors.
 *===========================================================================*/
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/*
 * Reducer will receive Words as key and list of page names in which the word is present as value. 
 * If the word has occurred N times in the page, along with the page names its frequency is also captured. 
 */
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

	private static final Logger LOG = Logger
			.getLogger(InvertedIndexReducer.class);

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		java.util.Map<String, Integer> dict = new HashMap<String, Integer>();
		int count = 0;
		StringBuffer buffer = new StringBuffer();
		for (Text value : values) {
			String str = value.toString();
			/*
			 * Check if page name is present in the HashMap ,if Page name is not
			 * present then add the Pagename to the HashMap and increment the
			 * counter by one , Else condition will be satisfied on first
			 * occurrence of that word
			 */
			if (dict != null && dict.get(str) != null) {
				count = (int) dict.get(str);
				dict.put(str, ++count);
			} else {
				dict.put(str, 1);
			}
		}
		/*
		 * Emit word and [page1→count of the word1 in page1 , page2→count of the
		 * word1 in page2 ………] as output
		 */
		LOG.info("Reducer key" + key + "value" + buffer.toString());
		context.write(key, new Text(dict.toString()));
	}
}
