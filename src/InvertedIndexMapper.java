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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/*
 * The Map class (an extension of Mapper) transforms key/value input into
 * intermediate key/value pairs to be sent to the Reducer.
 */
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Logger LOG = Logger
			.getLogger(InvertedIndexMapper.class);

	/*
	 * Map function reads the documents line by line and finds the body of the
	 * page and gets individual words excluding the special characters. As an
	 * output of mapper, we need to emit word as key and page name in which is
	 * present as value
	 */
	@Override
	public void map(LongWritable key, Text page, Context context)
			throws IOException, InterruptedException {

		Pattern linkPat = Pattern.compile("\\[\\[.*?]\\]");
		Pattern pattern = Pattern.compile("[^a-z A-Z]");
		String titleString;
		String textString;
		int startTitle = page.find("<title>");
		int endTitle = page.find("</title>", startTitle);
		int startText = page.find("<text");
		startText = page.find(">", startText);
		int endText = page.find("</text>", startText);
		if (startTitle == -1 || endTitle == -1 || startText == -1
				|| endText == -1) {
			titleString = "";
			textString = "";
		}
		// Excluding the title and text tags by adding their positions to the
		// indices
		startTitle += 7;
		startText += 1;

		// Parse the Text to Strings
		titleString = Text.decode(page.getBytes(), startTitle, endTitle
				- startTitle);
		textString = Text.decode(page.getBytes(), startText, endText
				- startText);
		String contents[] = textString.split(" ");
		for (int i = 0; i < contents.length; i++) {
			LOG.info("mapper key contents" + contents[i] + "value title"
					+ titleString);
			if (contents[i].startsWith("[[")) {
				Matcher m = linkPat.matcher(contents[i]); // extract outgoing links
				while (m.find()) { // loop on each outgoing link
					String url = m.group().replace("[[", "").replace("]]", "");
					if (!url.isEmpty()) {
						url = extractEscapeCharacters(url);
						url = getOnlyStrings(url);
						context.write(new Text(url), new Text(titleString));
					}
				}
			}
			if (!contents[i].startsWith("[[")) {
				contents[i] = extractEscapeCharacters(contents[i]);
				contents[i] = getOnlyStrings(contents[i]);
				context.write(new Text(contents[i]), new Text(titleString));
			}
		}
	}

	// Strip all the escape characters from the URLs
	public static String extractEscapeCharacters(String url) {
		url = (url.contains("&amp;")) ? url.replace("&amp;", "") : url;
		url = (url.contains("&quot;")) ? url.replace("&quot;", "") : url;
		url = (url.contains("&lt;br")) ? url.replace("&lt;br", "") : url;
		url = (url.contains("&lt;")) ? url.replace("&lt;", "") : url;
		url = (url.contains("&gt;")) ? url.replace("&gt;", "") : url;
		return url;
	}

	// Retrieve only the words from the links
	public static String getOnlyStrings(String s) {
		Pattern pattern = Pattern.compile("[^a-z A-Z]");
		Matcher matcher = pattern.matcher(s);
		String wordsOnly = matcher.replaceAll("");
		return wordsOnly;
	}
}

