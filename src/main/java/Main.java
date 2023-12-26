import java.sql.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/marmontDB";
    private static final String DB_BENUTZERNAME = "marmontDB";
    private static final String DB_PASSWORT = "marmontDB";
    private static final int MAX_NACHRICHTEN = 3;
    private static AtomicBoolean neueNachrichtEingefuegt = new AtomicBoolean(false);
    private static Queue<Integer> geleseneNachrichtenIds = new ConcurrentLinkedQueue<>();
    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        initializeDataSource();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        try {
            erstelleTabelleWennNichtVorhanden();

            Runnable leserTask = Main::leseNeuesteNachrichten;
            Runnable schreiberTask = Main::schreibeNachricht;

            executor.scheduleWithFixedDelay(leserTask, 0, 1, TimeUnit.SECONDS);
            executor.scheduleWithFixedDelay(schreiberTask, 0, 3, TimeUnit.SECONDS);

            executor.awaitTermination(MAX_NACHRICHTEN * 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Fehler im Executor-Service", e);
        } finally {
            executor.shutdown();
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    private static void initializeDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(DB_URL);
        config.setUsername(DB_BENUTZERNAME);
        config.setPassword(DB_PASSWORT);
        dataSource = new HikariDataSource(config);
    }

    private static void erstelleTabelleWennNichtVorhanden() {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS nachrichten (id SERIAL PRIMARY KEY, text VARCHAR(255), zeitstempel TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        try (Connection verbindung = dataSource.getConnection();
             PreparedStatement erstelleTabelle = verbindung.prepareStatement(createTableSQL)) {
            erstelleTabelle.execute();
        } catch (SQLException e) {
            logger.error("Fehler beim Erstellen der Tabelle", e);
        }
    }

    private static void leseNeuesteNachrichten() {
        if (!neueNachrichtEingefuegt.get()) {
            return;
        }
        String selectSQL = "SELECT * FROM nachrichten WHERE id > ?";
        try (Connection verbindung = dataSource.getConnection();
             PreparedStatement auswahl = verbindung.prepareStatement(selectSQL)) {
            int letzteGeleseneId = geleseneNachrichtenIds.isEmpty() ? 0 : geleseneNachrichtenIds.peek();
            auswahl.setInt(1, letzteGeleseneId);
            try (ResultSet ergebnis = auswahl.executeQuery()) {
                while (ergebnis.next()) {
                    int id = ergebnis.getInt("id");
                    if (!geleseneNachrichtenIds.contains(id)) {
                        String text = ergebnis.getString("text");
                        Timestamp zeitstempel = ergebnis.getTimestamp("zeitstempel");
                        logger.info("Gelesene Nachricht - ID: {}, Text: {}, Zeitstempel: {}", id, text, zeitstempel);
                        geleseneNachrichtenIds.add(id);
                        if (geleseneNachrichtenIds.size() > MAX_NACHRICHTEN) {
                            geleseneNachrichtenIds.poll();
                        }
                    }
                }
            }
            neueNachrichtEingefuegt.set(false);
        } catch (SQLException e) {
            logger.error("Fehler beim Lesen der neuesten Nachrichten", e);
        }
    }

    private static void schreibeNachricht() {
        String insertSQL = "INSERT INTO nachrichten (text) VALUES (?)";
        try (Connection verbindung = dataSource.getConnection();
             PreparedStatement preparedStatement = verbindung.prepareStatement(insertSQL)) {
            preparedStatement.setString(1, "Eine neue Nachricht");
            preparedStatement.executeUpdate();
            logger.info("Neue Nachricht eingefügt");
            neueNachrichtEingefuegt.set(true);
        } catch (SQLException e) {
            logger.error("Fehler beim Schreiben einer neuen Nachricht", e);
        }
    }
}
