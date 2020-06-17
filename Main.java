import com.sun.security.jgss.GSSUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Scraper implements Runnable {
    protected static Set<String> linksSyncedList = Collections.synchronizedSet(new HashSet<>());
    protected static Set<String> emailSyncedList = Collections.synchronizedSet(new HashSet<>());
    protected static Set<String> linksVisited = Collections.synchronizedSet(new HashSet<>());
    private Set<String> linkFilter = Collections.synchronizedSet(new HashSet<>());

    String hyperlink;
    Scraper(String hl) {
        this.hyperlink = hl;
    }

    @Override
    public void run() {

        try {
                Document document = Jsoup.connect(hyperlink).ignoreHttpErrors(true).get();

                linkFilter.addAll(document.select("a[href]").eachAttr("abs:href"));

                linkFilter.removeAll(linksVisited);

                linksSyncedList.addAll(linkFilter);


            Pattern regex = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z-.]+");
            Matcher matcher = regex.matcher(document.text());

            while (matcher.find()) {
                emailSyncedList.add(matcher.group());
                System.out.println(matcher.group());
            }
        } catch (IOException e) {
            e.printStackTrace();
            }
        try {
            Thread.sleep(2_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


public class Main {

    private static boolean batchReadyToUpload() {
        final int BATCH_SIZE = 10_000;
        return Scraper.emailSyncedList.size() >= BATCH_SIZE;
    }

    public static void dbUpload() {
        ArrayList<String> localCopy = new ArrayList<>(Scraper.emailSyncedList);
        String insertQuery;
        System.out.println("What is your database connection url?\n");
        Scanner keyboard = new Scanner(System.in);
        String databaseUrl = keyboard.next();
        String connectionUrl = databaseUrl;

        try(Connection conn = DriverManager.getConnection(connectionUrl);
            Statement statement = conn.createStatement() ) {

            synchronized (localCopy) {
                for(String link : localCopy) {
                    insertQuery = ("USE horowitz; INSERT INTO Emails VALUES ('" + link + "');");
                    statement.execute(insertQuery);
                    }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }



    public static void main(String[] args) throws InterruptedException {
        Scanner keyboard = new Scanner(System.in);
        System.out.println("What link will you start at?\n");
        String startingLink = keyboard.next();
        String link = startingLink;

        final int MAX_SIZE = 10_000;

        ExecutorService executor = Executors.newFixedThreadPool(500);

        synchronized (executor) {
            executor.execute(new Scraper(link));
            executor.wait(4_000);
        }
        while(Scraper.emailSyncedList.size() <= MAX_SIZE) {
            synchronized (Scraper.linksSyncedList) {
                if (!Scraper.linksSyncedList.isEmpty()) {
                    link = Scraper.linksSyncedList.iterator().next();
                    Scraper.linksSyncedList.remove(link);
                    Scraper.linksVisited.add(link);
                    executor.execute(new Scraper(link));
                    System.out.println(link);
                    System.out.println("Email list size: " + Scraper.emailSyncedList.size());
                }

                if (batchReadyToUpload()) {
                    dbUpload();
                    synchronized (executor) {
                        executor.wait(200_000);
                        executor.shutdownNow();
                    }
                }
            }
        }
    }
}
