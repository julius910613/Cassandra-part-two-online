import com.datastax.driver.core.*;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

/**
 * Created by fanchenli on 2/4/14.
 */
public class ClientLi {

    private Cluster cluster;

    private Session session;

    private PreparedStatement rowCQL = null;

    private PreparedStatement searchCQL = null;

    private final File dataDir = new File("/home/ubuntu/data/cassandra-test-dataset");
    //private final File dataDir = new File("/home/fanchenli/Downloads");

    private final File logFile = new File(dataDir, "CSC8101-logfile.gz");
    //private final File logFile = new File(dataDir, "loglite");

    private final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");

    public void createSchema() {
        //create keyspace

        cluster = Cluster.builder().addContactPoint("ec2-54-194-196-168.eu-west-1.compute.amazonaws.com").build();


        final Session bootstartupSession = cluster.connect();
        String keyspaceCQL = "CREATE KEYSPACE LI_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1}";

        bootstartupSession.execute(keyspaceCQL);
        bootstartupSession.shutdown();

        session = cluster.connect("li_keyspace");

        // session.execute(keyspaceCQL);
        System.out.print("\n user1 keyspace created");


        String tableCQL = "CREATE TABLE LI_keyspace.URLRecords (" +
                "Client_id int, " +
                "TimeStamp timestamp, " +
                "Action text, " +
                "Status text," +
                "Size text," +
                "PRIMARY KEY (Client_id, TimeStamp, Action)" +
                ");";
        session.execute(tableCQL);
        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
                "VALUES (? ,? ,? ,? ,?);");

        System.out.println("table created");

    }


    public void insertRow(int clientID, Date timestamp, String action, String status, String size) {

        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(rowCQL).bind(clientID, timestamp, action, status, size));
        // System.out.print("\n data has been inserted");
    }


    public void countActivity(String c_id, String S_time, String E_time) {
        //String countCQL = "select url, client_id from client_table where client_id = " + " ' "+c_id + " ' " + "and timespot >=" + " ' " + S_time + " ' " + " and timespot <=' " + E_time + " ';";
        PreparedStatement countCQL = session.prepare("SELECT url, client_id FROM client_table WHERE client_id = ? and timespot >= ? and timespot <= ? ");
        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(c_id, S_time, E_time));

        ResultSet rset = queryFuture.getUninterruptibly();
        for (Row row : rset) {
            System.out.println(row.getString(0) + " " + row.getString(1));
        }


    }


    public void countNumOfURL(ArrayList<String> URLlist, String s_time, String e_time) {
        PreparedStatement countCQL = session.prepare("SELECT count(*) FROM url_table WHERE url = ? and timespot >= ? and timespot <= ? ");
        for (int i = 0; i < URLlist.size(); i++) {
            String url = URLlist.get(i);
            //String countCQL = "select count(*) from url_table where url = ' " + url + " ' and timespot >=" + " ' " + s_time + " ' " + " and timespot <=' " + e_time + " ';";

            ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(countCQL).bind(URLlist.get(i), s_time, e_time));

            ResultSet rset = queryFuture.getUninterruptibly();
            for (Row row : rset) {
                System.out.println(url + " " + row.getLong(0));
            }
        }
    }

    public void connect() {
        //cluster = Cluster.builder().addContactPoint("ec2-54-194-196-168.eu-west-1.compute.amazonaws.com").build();
        cluster =  Cluster.builder().addContactPoint("ec2-54-194-161-1.eu-west-1.compute.amazonaws.com").build();
        session = cluster.connect("li_keyspace");

        rowCQL = session.prepare("INSERT INTO LI_keyspace.URLRecords(Client_id, TimeStamp, Action, Status, Size)" +
                "VALUES (? ,? ,? ,? ,?);");

        searchCQL = session.prepare("INSERT INTO LI_keyspace.SessionRecords (Client_id, StartTime, EndTime, numberOfAccess, numberOfURL)  VALUES (? ,? ,? ,? ,?);");
    }


    public void CreateSessionTable() {
        cluster =  Cluster.builder().addContactPoint("ec2-54-194-161-1.eu-west-1.compute.amazonaws.com").build();
        //cluster = Cluster.builder().addContactPoint("localhost").build();
        session = cluster.connect("li_keyspace");
        String tableCQL = "CREATE TABLE LI_keyspace.SessionRecords (" +
                "Client_id int, " +
                "StartTime timestamp, " +
                "EndTime timestamp, " +
                "numberOfAccess int," +
                "numberOfURL int," +
                "PRIMARY KEY (Client_id, StartTime, EndTime)" +
                ");";
        session.execute(tableCQL);

        searchCQL = session.prepare("INSERT INTO LI_keyspace.SessionRecords (Client_id, StartTime, EndTime, numberOfAccess, numberOfURL)  VALUES (? ,? ,? ,? ,?);");

    }


    public void CountNumber(int clientID) throws IOException, ParseException {
        int count = 0, globalCount = 0;
        boolean inti = true;
        SiteSession ss = new SiteSession();


        final FileInputStream fileInputStream = new FileInputStream(logFile);
        //final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        final InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line = null;
        while ((line = bufferedReader.readLine()) != null && globalCount < 1000000) {
            final String[] tokens = line.split(" ");
            if (tokens.length == 8) {

                globalCount++;

                String id = tokens[0];

                String timestampString = tokens[1] + " " + tokens[2];

                Date date = dateFormat.parse(timestampString);

                long firstHitMillis = date.getTime();

                String urlString = tokens[4];

                String url = tokens[4];


                if (inti == true) {
                    ss.setID(id);
                    ss.setFirstHitMillis(firstHitMillis);
                    //ss.update(firstHitMillis,url);
                    ss.addHitCount(firstHitMillis, url);
                    inti = false;
                } else {
                    if (id.equals(ss.getId())) {
                        ss.update(firstHitMillis, url);
                        if (ss.getflagNormal()) {
                            ss.addHitCount(firstHitMillis, url);
                        }
                    }

                    if (ss.getTimeOut() || id.equals(ss.getId()) == false) {
                        System.out.println("Session expired! " + "Start time: " + ss.getFirstHitMillis() + " End time: " + ss.getLastHitMillis() + " number of access: " + ss.getHitCount() + " number of url access: " + ss.getHyperLogLog().cardinality());
                        Date startdate = new Date(ss.getFirstHitMillis());
                        Date enddate = new Date(ss.getLastHitMillis());
                        int numofaccess = (int) ss.getHitCount();
                        int numofurl = (int) ss.getHyperLogLog().cardinality();
                        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchCQL).bind(clientID, startdate, enddate, numofaccess, numofurl));
                        count++;


                        System.out.println(count + " of session has been insert");


                        ss.reset();

                        ss.setID(id);
                        ss.setFirstHitMillis(firstHitMillis);
                        //ss.update(firstHitMillis,url);
                        ss.addHitCount(firstHitMillis, url);
                    }

                }
            } else {
                System.out.println("not correct client id");
            }


        }


    }


    public void insertSession() throws ParseException, IOException {



        final AtomicReference<SiteSession> expiredSession = new AtomicReference<>(null);
        HashMap<String, SiteSession> sessions = new LinkedHashMap<String, SiteSession>() {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                SiteSession siteSession = (SiteSession) eldest.getValue();
                boolean shouldExpire = siteSession.isExpired();
                if (shouldExpire) {
                    expiredSession.set(siteSession);
                    SiteSession sessionOutput = expiredSession.get();
                    Date startdate = new Date(sessionOutput.getFirstHitMillis());
                    Date enddate = new Date(sessionOutput.getLastHitMillis());
                    int numofaccess = (int) sessionOutput.getHitCount();
                    int numofurl = (int) sessionOutput.getHyperLogLog().cardinality();
                    ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchCQL).bind(Integer.parseInt(sessionOutput.getId()), startdate, enddate, numofaccess, numofurl));
                   // sessions.remove(id);
                   // count++;

                    //if(count % 100000 == 0){
                   // System.out.println(count + " of session has been input");
                    //}

                }
                return siteSession.isExpired();
            }
        };

        int count = 0, globalCount = 0;
        boolean inti = true;



        final FileInputStream fileInputStream = new FileInputStream(logFile);
        final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
        final InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
        final BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String line = null;
        while ((line = bufferedReader.readLine()) != null ) {
            final String[] tokens = line.split(" ");
            if (tokens.length == 8) {

                globalCount++;

                String id = tokens[0];

                String timestampString = tokens[1] + " " + tokens[2];

                Date date = dateFormat.parse(timestampString);

                long firstHitMillis = date.getTime();

                //String urlString = tokens[4];

                String url = tokens[4];


                if (sessions.containsKey(id) == false){
                    SiteSession ss = new SiteSession();
                    ss.setID(id);
                    ss.setFirstHitMillis(firstHitMillis);
                    ss.update(firstHitMillis,url);
                    ss.addHitCount(firstHitMillis, url);

                    sessions.put(id,ss);


                }
                else{
                    SiteSession s1 = sessions.get(id);
                   SiteSession.setGlobalLastHitMillis(firstHitMillis);
                    if(s1.isExpired()){

                        SiteSession sessionOutput = s1;
                        Date startdate = new Date(sessionOutput.getFirstHitMillis());
                        Date enddate = new Date(sessionOutput.getLastHitMillis());
                        int numofaccess = (int) sessionOutput.getHitCount();
                        int numofurl = (int) sessionOutput.getHyperLogLog().cardinality();
                        ResultSetFuture queryFuture = session.executeAsync(new BoundStatement(searchCQL).bind(Integer.parseInt(id), startdate, enddate, numofaccess, numofurl));
                        sessions.remove(id);
                        count++;

                        if(count % 10000000 == 0){
                            System.out.println(count + " of session has been input");
                        }

                    }

                    else{
                        s1.addHitCount(firstHitMillis, url);
                        sessions.put(id, s1);
                    }
                }
            }
        }
        System.out.println("done");
    }


    public void close() {
        cluster.shutdown();
    }


}
