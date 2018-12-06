package com.leicki.mavenetlfiles;
/**
 * @author leickiet
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.*;


public class CreateTable {
    
    private static String tableStructure;
    private static String columnsName;
    private static String targetStructure;
    private static int countColumns;
    
    public CreateTable(String pathFile, String tableName, String LoadDate){
        try {
            createNewTable(pathFile, tableName, LoadDate);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CreateTable.class.getName()).log(Level.SEVERE, null, ex);
        }    
    }
    
    public static void createNewTable(String pathFile, String tableName, String LoadDate) throws FileNotFoundException{
        String PathNewFile = pathFile;
        boolean firstColumn = true;
        String column_name;
        int colNumber = 0;
        BufferedReader br = new BufferedReader(new FileReader(PathNewFile));
        
        String nomedoarquivo = PathNewFile.substring(PathNewFile.lastIndexOf("\\")+1,PathNewFile.length());
        String NewTable = "CREATE TABLE leicki." + tableName + "( ";
        columnsName = "(";
        targetStructure = "(";
        try { 
            String linha = br.readLine().trim().toUpperCase().replace("%", "").replace(" ", "_").replace("-", "_").replace("#", "").replace("(","").replace(")", "").replace("'", "").replace("?","").replace("+","").replace("/", "_").replace("\\","_");
            
           System.out.println(linha);
           
           String[] arr = linha.split("(?x)   " + 
                     ",          " +   // Split on comma
                     "(?=        " +   // Followed by
                     "  (?:      " +   // Start a non-capture group
                     "    [^\"]* " +   // 0 or more non-quote characters
                     "    \"     " +   // 1 quote
                     "    [^\"]* " +   // 0 or more non-quote characters
                     "    \"     " +   // 1 quote
                     "  )*       " +   // 0 or more repetition of non-capture group (multiple of 2 quotes will be even)
                     "  [^\"]*   " +   // Finally 0 or more non-quotes
                     "  $        " +   // Till the end  (This is necessary, else every comma will satisfy the condition)
                     ")          "     // End look-ahead
                         );
 
            String columnFind[] = new String[500];
            int colNum;
            
            for (String columnName : arr){
                if(firstColumn){
                    firstColumn = false;
                }else{
                    NewTable = NewTable + ",";
                    columnsName = columnsName + ",";
                    targetStructure = targetStructure + ",";
                }
                column_name = columnName;
                column_name = column_name.replace(",", "").replace("\"", "");
                column_name = column_name.substring(0,1).replace("_", "") + column_name.substring (1);
                
                column_name = column_name.substring (0,column_name.length()-1) + column_name.substring(column_name.length()-1).replace("_", ""); 
                column_name = column_name.substring (0,column_name.length()-1) + column_name.substring(column_name.length()-1).replace("_", ""); 
                column_name = column_name.substring (0,column_name.length()-1) + column_name.substring(column_name.length()-1).replace("_", ""); 
                
                column_name = column_name.replaceAll("[^\\p{ASCII}]", "");
                
                column_name = column_name.replace("__", "_");
                column_name = column_name.replace("__", "_");
                
                colNum = 1;
                
                for(int cont=0; cont<=colNumber; cont++){
                    if(column_name.equals(columnFind[cont])){
                        column_name = column_name + colNum;
                        colNum++;
                        cont=0;
                    }
                }
                
                columnFind[colNumber]= column_name;
                
                //column_name = "C" + colNumber + "_" + column_name;
                
                NewTable = NewTable + "\"" + column_name + "\" VARCHAR2(4000 char) \n ";
                targetStructure = targetStructure + "?";
                columnsName = columnsName + "\"" + column_name + "\"";
                
                colNumber++;
            }
            NewTable = NewTable + " ,LINE_NUMBER VARCHAR2(10) , FILE_NAME VARCHAR2(2000 char), LOAD_DATE DATE)";
            tableStructure = NewTable;
            columnsName = columnsName + ",LINE_NUMBER, FILE_NAME, LOAD_DATE)";
            targetStructure = targetStructure + ",?, '" + nomedoarquivo + "', to_date('" + LoadDate +"','yyyymmdd'))";
            countColumns = colNumber;
			
        } catch (IOException ex) {
            Logger.getLogger(CreateTable.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
        
    public String getTableStructure(){
        return tableStructure;
    } 
    
    public String getTargetStructure(){
        return targetStructure;
    }
    
    public String getColumnsName(){
        return columnsName;
    }
    
    public int getcountColumns(){
        return countColumns;
    }
}