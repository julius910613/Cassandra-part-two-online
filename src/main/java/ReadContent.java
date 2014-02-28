import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;


/**
 * Created by fanchenli on 2/11/14.
 */
public class ReadContent {
     private final File dataDir = new File("/home/ubuntu/data/cassandra-test-dataset");

    private final File logFile = new File(dataDir, "CSC8101-logfile.gz");
    private int count;

    private ClientLi cl = new ClientLi();

    ReadContent() {
        cl.connect();
        count = 0;
    }

    private final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");


    public void readLine() throws IOException, ParseException {
        try (
                final FileInputStream fileInputStream = new FileInputStream(logFile);
                final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                final InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
                final BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] tokens = line.split(" ");
                if (tokens.length == 8) {
                    String clientIDString = tokens[0];
                    int clientID = Integer.parseInt(clientIDString);

                    String timestampString = tokens[1] + " " + tokens[2];

                    Date date = dateFormat.parse(timestampString);

                    long millis = date.getTime();

                    String methodString = tokens[3].substring(1);

                    String urlString = tokens[4];
                    String versionString = tokens[5].substring(0, tokens[5].length() - 1);

                    String actionString = methodString + " " + urlString + " " + versionString;

                    String statusString = tokens[6];

                    String sizeString = tokens[7];

                    cl.insertRow(clientID, date, actionString, statusString, sizeString);
                    count++;
                    if (count % 1000000 == 0) {
                        System.out.println(count + "data has been insert");
                    }

                }

            }
            System.out.println("done!");


            //assertEquals(893971817000L, millis); // 30/Apr/1998:21:30:17 +0000
        }
    }

    public static void main(String[] args) {

        ReadContent rd = new ReadContent();
        ClientLi cl = new ClientLi();
        cl.CreateSessionTable();

        try {
            rd.readLine();
            cl.insertSession();
            cl.getSessionDetails(34600);
            cl.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
