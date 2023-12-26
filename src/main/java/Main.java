import java.sql.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/marmontDB";
    private static final String DB_BENUTZERNAME = "marmontDB";
    private static final String DB_PASSWORT = "marmontDB";
    private static final int MAX_NACHRICHTEN = 3;
    private static volatile boolean neueNachrichtEingefuegt = false;
    private static Queue<Integer> geleseneNachrichtenIds = new LinkedList<>();

    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        try (Connection verbindung = DriverManager.getConnection(DB_URL, DB_BENUTZERNAME, DB_PASSWORT)) {
            erstelleTabelleWennNichtVorhanden(verbindung);

            Runnable leserTask = () -> leseNeuesteNachrichten(verbindung);
            Runnable schreiberTask = () -> schreibeNachricht(verbindung);

            executor.scheduleWithFixedDelay(leserTask, 0, 1, TimeUnit.SECONDS);
            executor.scheduleWithFixedDelay(schreiberTask, 0, 3, TimeUnit.SECONDS);

            executor.awaitTermination(MAX_NACHRICHTEN * 5, TimeUnit.SECONDS);
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static void erstelleTabelleWennNichtVorhanden(Connection verbindung) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS nachrichten (id SERIAL PRIMARY KEY, text VARCHAR(255), zeitstempel TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        try (PreparedStatement erstelleTabelle = verbindung.prepareStatement(createTableSQL)) {
            erstelleTabelle.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void leseNeuesteNachrichten(Connection verbindung) {
        if (!neueNachrichtEingefuegt) {
            return;
        }
        String selectSQL = "SELECT * FROM nachrichten WHERE id > ?";
        try (PreparedStatement auswahl = verbindung.prepareStatement(selectSQL)) {
            int letzteGeleseneId = geleseneNachrichtenIds.isEmpty() ? 0 : geleseneNachrichtenIds.peek();
            auswahl.setInt(1, letzteGeleseneId);
            ResultSet ergebnis = auswahl.executeQuery();

            while (ergebnis.next()) {
                int id = ergebnis.getInt("id");
                if (!geleseneNachrichtenIds.contains(id)) {
                    String text = ergebnis.getString("text");
                    Timestamp zeitstempel = ergebnis.getTimestamp("zeitstempel");
                    System.out.println("Gelesene Nachricht - ID: " + id + ", Text: " + text + ", Zeitstempel: " + zeitstempel);
                    geleseneNachrichtenIds.add(id);
                    if (geleseneNachrichtenIds.size() > MAX_NACHRICHTEN) {
                        geleseneNachrichtenIds.poll(); // Ältestes Element entfernen
                    }
                }
            }
            neueNachrichtEingefuegt = false;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void schreibeNachricht(Connection verbindung) {
        String insertSQL = "INSERT INTO nachrichten (text) VALUES (?)";
        try (PreparedStatement preparedStatement = verbindung.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, "Eine neue Nachricht");
            preparedStatement.executeUpdate();
            System.out.println("Neue Nachricht eingefügt");
            neueNachrichtEingefuegt = true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
