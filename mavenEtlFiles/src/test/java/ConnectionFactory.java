import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author leickiet
 */

public class ConnectionFactory {
	public static java.sql.Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            String url = "jdbc:oracle:thin:@IP:PORTA/SID";
            String username = "user"; 
            String password = "senha";
            connection = DriverManager.getConnection(url, username, password);
            System.out.println("Conexao ok");
            return connection;
        } catch (ClassNotFoundException e) {            
            System.out.println("O driver especificado nao foi encontrado.");
            return null;
        } catch (SQLException e) {
            System.out.println("Nao foi possivel conectar ao banco de dados." + e.getMessage());
            return null;
        }
    }
}
