
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.*; 
import com.leicki.mavenetlfiles.*;

/**
 * @author leickiet
 */
public class EtlFiles {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
            
        try {
            
            Connection conn = ConnectionFactory.getConnection();
            
           // bulkInsertCSVtoOracle bkInsertCSVtoOracle = new bulkInsertCSVtoOracle();
            BulkInsertCSVtoOracle.InsertCSVtoOracle("LEICKI","HOOVERS_8M_CLEAN", "C:\\Leicki\\hoovers\\export_hoovers_8m_clean.csv", conn, "20181206");
            
        } catch (Exception ex) {
            Logger.getLogger(EtlFiles.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
