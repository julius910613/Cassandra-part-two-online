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
    //private final File dataDir = new File("/home/fanchenli/Downloads");
    private final File dataDir = new File("/home/ubuntu/data/cassandra-test-dataset");
    // 1,352,794,346 lines, 13050324662bytes (13G), md5sum=b7089321366fe6f8131196b81d060c5d
    // first line: 34600 [30/Apr/1998:21:30:17 +0000] "GET /images/hm_bg.jpg HTTP/1.0" 200 24736

    // last line:  515626 [26/Jul/1998:21:59:55 +0000] "GET /english/images/team_hm_header.gif HTTP/1.1" 200 763
   // private final File logFile = new File(dataDir, "loglite");
   // cassandra-test-dataset
    private final File logFile = new File(dataDir, "CSC8101-logfile.gz");
    private int count;

    private ClientLi cl = new ClientLi();
     ReadContent(){
         //cl.createSchema();
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
            while((line = bufferedReader.readLine()) != null){
                final String[] tokens = line.split(" ");
                if(tokens.length == 8){
                    String clientIDString  = tokens[0];
                    int clientID = Integer.parseInt(clientIDString);

                    // System.out.println(clientIDString);

                    String timestampString = tokens[1] + " "  + tokens[2];

                    Date date = dateFormat.parse(timestampString);

                    long millis = date.getTime();
                    //System.out.println(millis);




                    String methodString = tokens[3].substring(1);

                    // S/ystem.out.println(methodString);


                    String urlString = tokens[4];

                    //System.out.println(urlString);

                    String versionString = tokens[5].substring(0, tokens[5].length() - 1);

                    //System.out.println(versionString);

                    String actionString = methodString + " " + urlString + " " + versionString;

                    String statusString = tokens[6];

                    //System.out.println(statusString);

                    String sizeString = tokens[7];

                    //System.out.println(sizeString);

                    cl.insertRow(clientID, date,actionString, statusString, sizeString);
                    count ++;
                    if(count % 1000000 == 0){
                        System.out.println(count + "data has been insert");
                    }
                    //System.out.println(count + "data has been insert");

                }
                //assertEquals(5, tokens.length);


            }
            System.out.println("done!");



            //assertEquals(893971817000L, millis); // 30/Apr/1998:21:30:17 +0000
        }
    }

    public static void  main(String[] args){

        ReadContent rd = new ReadContent();

        try {
            rd.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

      //  ClientLi cl  = new ClientLi();
      //  cl.connect();
       // cl.CountNumber(34600);
    }

}
