package com.leicki.mavenetlfiles;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author leickiet
 */
public class CreateTableOracle {
            
    public void CreateTableOracle(String sqlCreateTable, String tableName, String owner, Connection conn) throws SQLException{
         //   Connection conn = ConnectionFactory.getConnection();
            Statement stmt;
            stmt = conn.createStatement();
            
            System.out.println(sqlCreateTable);
       
            try{
                stmt.executeUpdate("DROP TABLE " + owner + "." + tableName );
            }catch(SQLException e)
            {System.out.println("A Tabela " + owner + "." + tableName + " não existe");}
            
            stmt.executeUpdate(sqlCreateTable);
    }
    
}
