import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.util.Properties;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);
    private static final int MAX_NACHRICHTEN = 1;
    private static final int SCHREIBER_DELAY = 3;
    private static final int LESER_DELAY = 1;
    private static final int TERMINATION_DELAY = MAX_NACHRICHTEN * 5;
    private static AtomicBoolean neueNachrichtEingefuegt = new AtomicBoolean(false);
    private static Queue<Integer> geleseneNachrichtenIds = new ConcurrentLinkedQueue<>();
    private static HikariDataSource dataSource;

    public static void main(String[] args) {
        Properties properties = loadProperties();
        initializeDataSource(properties);

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        try {
            erstelleTabelleWennNichtVorhanden();

            Runnable schreiberTask = Main::schreibeNachricht;
            Runnable leserTask = Main::leseNeuesteNachrichten;

            executor.scheduleWithFixedDelay(schreiberTask, 0, SCHREIBER_DELAY, TimeUnit.SECONDS);
            executor.scheduleWithFixedDelay(leserTask, 0, LESER_DELAY, TimeUnit.SECONDS);

            executor.awaitTermination(TERMINATION_DELAY, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Fehler im Executor-Service", e);
        } finally {
            executor.shutdown();
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream("db.properties"));
        } catch (IOException e) {
            logger.error("Fehler beim Laden der Datenbankkonfigurationsdatei", e);
        }
        return properties;
    }

    private static void initializeDataSource(Properties properties) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.getProperty("db.url"));
        config.setUsername(properties.getProperty("db.username"));
        config.setPassword(properties.getProperty("db.password"));
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
        if (!neueNachrichtEingefuegt.getAndSet(false)) {
            return;
        }
        String selectSQL = "SELECT * FROM nachrichten WHERE id > ? ORDER BY id ASC LIMIT ?";
        try (Connection verbindung = dataSource.getConnection();
             PreparedStatement auswahl = verbindung.prepareStatement(selectSQL)) {
            int letzteGeleseneId = geleseneNachrichtenIds.isEmpty() ? 0 : geleseneNachrichtenIds.peek();
            auswahl.setInt(1, letzteGeleseneId);
            auswahl.setInt(2, MAX_NACHRICHTEN);
            try (ResultSet ergebnis = auswahl.executeQuery()) {
                while (ergebnis.next()) {
                    int id = ergebnis.getInt("id");
                    String text = ergebnis.getString("text");
                    Timestamp zeitstempel = ergebnis.getTimestamp("zeitstempel");
                    logger.info("Gelesene Nachricht - ID: {}, Text: {}, Zeitstempel: {}", id, text, zeitstempel);
                    geleseneNachrichtenIds.add(id);
                }
                geleseneNachrichtenIds.poll();
            }
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
