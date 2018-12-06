package com.leicki.mavenetlfiles;

import com.opencsv.CSVReader;
import java.io.*;
import java.sql.*; 
import java.util.List;



public class BulkInsertCSVtoOracle {  
    
    
        public static void InsertCSVtoOracle(String owner, String tableName, String pathFile, Connection conn, String LoadDate) throws Exception{                
          
            CreateTable newTable;
            newTable = new CreateTable(pathFile, tableName, LoadDate);
            String sql;
            sql = newTable.getTableStructure();
            
            CreateTableOracle table = new CreateTableOracle();
            table.CreateTableOracle(sql, tableName, owner, conn);
            
            String columnsName = newTable.getColumnsName();
            String targetStructure =  newTable.getTargetStructure();
            String [] nextLine;
            
            int i;
            int count = newTable.getcountColumns();
            int lnNum = 0;
            //Connection conn = ConnectionFactory.getConnection();
            Statement stmt;      
            String jdbc_insert_sql = "INSERT INTO " + owner + "." + tableName 
                    + columnsName 
                    + " VALUES"
                    + targetStructure;


                    /* Read CSV file in OpenCSV */
                    System.out.println(jdbc_insert_sql);
                try (PreparedStatement sql_statement = conn.prepareStatement(jdbc_insert_sql)) {
                    /* Read CSV file in OpenCSV */
                    sql_statement.setFetchSize(500);
                    String inputCSVFile = pathFile;
                    CSVReader reader = new CSVReader(new FileReader(inputCSVFile));
                    
                    try{
                        nextLine = reader.readNext();
                    }catch(Exception e){
                        System.out.println("registro = " + lnNum + " com erro." + e);
                    }
                    
                    //loop file , add records to batch
                    
                    int commit=0;
                    String registro =null;
                    while ((nextLine = reader.readNext()) != null) {
                        lnNum++;
                        
                        /* Bind CSV file input to table columns */
                        for(i=0; i <= count; i++){
                            try{
                                
                                if(i==count) {
                                  registro = "" + lnNum;  
                                }else{
                                    if(nextLine[i].length()>4000){
                                        registro = nextLine[i].substring(0,4000);
                                    }else{
                                        registro = nextLine[i]; 
                                    }
                                }
                                sql_statement.setString(i+1, registro);
                            }catch(Exception e){
                               System.out.println("Linha " + lnNum + " Com erro. Verifique o registro: " + nextLine[0]); 
                               i=count;
                            }
                        }
                        try{
                            sql_statement.addBatch();
                        }catch(Exception erro){
                            System.out.println("registro = " + lnNum + " com erro.");
                            System.out.println("registro = " + nextLine[0] + " //// " + nextLine[1]);
                        }
                        
                        if(commit == 500){
                            try {
                                sql_statement.executeBatch();
                            } catch(Exception e) {
                                System.out.println(e);
                            }
                            commit = 0;
                        }
                        commit++;
                    }
         
                    //We are now ready to perform a bulk batch insert
                    int[] totalRecords = new int[7];
                    try {
                        totalRecords = sql_statement.executeBatch();
                    } catch(BatchUpdateException e) {
                        //you should handle exception for failed records here
                        totalRecords = e.getUpdateCounts();
                        System.out.println(e);
                    } catch(SQLException erro){
                       System.out.println(erro); 
                    }
                    System.out.println ("Total records inserted in bulk from CSV file " + totalRecords.length);
                   }
                /* Close connection */
                }
}