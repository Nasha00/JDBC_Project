/**
 * CSE 132A       Winter 2016
 * Assignment #2(JDBC)
 * Due Monday, Feb 29th
 * Name: Nasha Zhai
 * Login: cs132adm
 * PID: A53082522
 */
import java.sql.*;

public class PA2 {
    public static void main( String[] args)
    {
        // Database connection.
        Connection conn = null;
        
        try {
            // Load the JDBC class.
            Class.forName("org.sqlite.JDBC");
            // Get the connection to the database.
            // - "jdbc" : JDBC connection name prefix.
            // - "sqlite" : The concrete database implementation
            // (e.g., sqlserver, postgresql).
            // - "pa2.db" : The name of the database. In this project,
            // we use a local database named "pa2.db". This can also
            // be a remote database name.
            conn = DriverManager.getConnection("jdbc:sqlite:pa2.db");
            System.out.println("Opened database successfully.");
            
            // Get a Statement object.
            Statement stmt = conn.createStatement();
            // Get a ResultSet object.
            ResultSet rset = null;
            
            // Create and populate the current table.
            stmt.executeUpdate("DROP TABLE IF EXISTS CurrT;");
            stmt.executeUpdate("CREATE TABLE CurrT(Airline char(32), Origin char(32), Destination char(32), Stops integer DEFAULT 0);");
            stmt.executeUpdate("INSERT INTO CurrT(Airline, Origin, Destination) SELECT * FROM FLIGHT;");
            // Create and populate the old table.
            stmt.executeUpdate("DROP TABLE IF EXISTS OldT;");
            stmt.executeUpdate("CREATE TABLE OldT(Airline char(32), Origin char(32), Destination char(32), Stops integer DEFAULT 0);");
            stmt.executeUpdate("INSERT INTO OldT(Airline, Origin, Destination) SELECT * FROM FLIGHT;");
            // Create and populate table Delta.
            stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
            stmt.executeUpdate("CREATE TABLE Delta(Airline char(32), Origin char(32), Destination char(32), Stops integer DEFAULT 0);");
            stmt.executeUpdate("INSERT INTO Delta(Airline, Origin, Destination) SELECT * FROM FLIGHT;");
            
            rset = stmt.executeQuery("SELECT COUNT(*) AS NUM FROM Delta;");
            
            // while Delta is not empty.
            while (rset.getInt("NUM") > 0) {
                // Update old table.
                stmt.executeUpdate("DELETE FROM OldT;");
                stmt.executeUpdate("INSERT INTO OldT SELECT * FROM CurrT;");
                
                // Update current table.
                stmt.executeUpdate("DELETE FROM CurrT;");
                stmt.executeUpdate("INSERT INTO CurrT SELECT * FROM OldT UNION SELECT f.Airline, f.Origin, d.Destination, d.Stops + 1 FROM Flight f, Delta d WHERE f.Airline = d.Airline AND f.Destination = d.Origin AND f.Origin <> d.Destination AND NOT EXISTS (SELECT * FROM OldT t WHERE t.Origin = d.Origin AND t.Destination = d.Destination AND t.Airline = d.Airline AND t.Stops <> d.Stops);" );
                // Update Delta.
                stmt.executeUpdate("DELETE FROM Delta;");
                stmt.executeUpdate("INSERT INTO Delta SELECT * FROM CurrT EXCEPT SELECT * FROM OldT;");
                
                rset = stmt.executeQuery("SELECT COUNT(*) AS NUM FROM Delta;");
            }
            
            // Create solution table Connected.
            stmt.executeUpdate("DROP TABLE IF EXISTS Connected;");
            stmt.executeUpdate("CREATE TABLE Connected(Airline char(32), Origin char(32), Destination char(32), Stops integer);");
            stmt.executeUpdate("INSERT INTO Connected SELECT Airline, Origin, Destination, MIN(Stops) AS Stops FROM CurrT GROUP BY Airline, Origin, Destination;");
            
            // Drop temporary tables.
            stmt.executeUpdate("DROP TABLE IF EXISTS CurrT;");
            stmt.executeUpdate("DROP TABLE IF EXISTS OldT;");
            stmt.executeUpdate("DROP TABLE IF EXISTS Delta;");
            
            // Close the Result and Statement objects.
            rset.close();
            stmt.close();
            
        }
        catch (Exception e) {
            throw new RuntimeException("There was a runtime problem!", e);
        }
        finally {
            try {
                if (conn != null) conn.close();
            }
            catch (SQLException e) {
                throw new RuntimeException("Cannot close the connection!", e);
            }
        }
    }
}
