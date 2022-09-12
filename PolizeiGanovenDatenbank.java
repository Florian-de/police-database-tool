import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.*;
import java.util.*;
import java.lang.*;
/**
 * Beschreiben Sie hier die Klasse Ganovenliste.
 * 
 * @author Florian Dreyer
 * @version 08.02.2022
 */
public class PolizeiGanovenDatenbank
{

    public PolizeiGanovenDatenbank()
    {
        
    }

    private Connection connect() {
        Connection conn = null;
        try {
            String url = "jdbc:sqlite:" + "prjkt.db";
            conn = DriverManager.getConnection(url);
            //System.out.println("Connection to SQLite has been established.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } 
        return conn;
    }

    //fügt neuen Ganoven zu DB hinzu, wohnort optional
    public int ganoveAnlegen(String name, String wohnort) {
        if(wohnort == ""){
            wohnort = "Unbekannt";
        }
        Connection conn = connect();
        if (conn==null) {
            return 0;
        }
        String sqlInsert = "INSERT INTO Verbrecher (Name, Wohnort) VALUES (?, ?)";
        int returnValue = 0;
        try (
        PreparedStatement pStmt = conn.prepareStatement(sqlInsert)) {
            pStmt.setString(1, name); 
            pStmt.setString(2, wohnort);
            pStmt.executeUpdate();
            System.out.println(System.lineSeparator() + "Der neue Eintrag hat die ID " + pStmt.getGeneratedKeys().getInt(1) + " bekommen.");
            returnValue = pStmt.getGeneratedKeys().getInt(1);
            conn.close();        
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return returnValue;
    }

    //Gibt Ganovenname zu id aus
    public String getName(int id){
        Connection conn = connect();
        if (conn==null) {
            return "Error";
        }
        String sqlSelect = "SELECT Name FROM Verbrecher WHERE ID = ?"; 
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
        ) {
            pStmt.setString(1, Integer.toString(id));         
            ResultSet rs = pStmt.executeQuery();    
            String name = rs.getString("Name");
            rs.close();
            conn.close();
            return name;
        }
        catch (SQLException e) {       
            System.out.println(e.getMessage());
        }
        return "Error";

    }
    
    //Gibt id zu Name aus
    public int getId(String name){
        Connection conn = connect();
        if (conn==null) {
            return 0;
        }
        String sqlSelect = "SELECT ID FROM Verbrecher WHERE Name = ?"; 
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
        ) {
            pStmt.setString(1, name);
            ResultSet rs = pStmt.executeQuery();    
            String id = rs.getString("ID");
            rs.close();
            conn.close();
            return Integer.parseInt(id);
        }
        catch (SQLException e) {       
            System.out.println(e.getMessage());
        }
        return 0;

    }

    //neue Haftzeit eines Ganoven wird in DB gespeichert
    public void haftzeitHinzufuegen(int id, int haftbeginn, int haftende) {
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlInsert = "INSERT INTO Haftzeit (ID, Haftbeginn, Haftende) VALUES (?, ?, ?)";
        try (
        PreparedStatement pStmt = conn.prepareStatement(sqlInsert)) {
            pStmt.setString(1, Integer.toString(id)); 
            pStmt.setString(2, Integer.toString(haftbeginn));
            pStmt.setString(3, Integer.toString(haftende));
            pStmt.executeUpdate();
            System.out.println(System.lineSeparator()+"Die neue Haft des Ganoven " + getName(id) + " mit dem Haftbeginn " +  datum(haftbeginn) + " und dem Haftende " + datum(haftende) + " wurde hinzugefügt");
            conn.close();        
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //neue Spezialität eines Ganoven wird in DB gespeichert
    public void spezialitaetHinzufuegen(int id, String spezialitaet) {
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlInsert = "INSERT INTO Spezialitäten (ID, Spezialität) VALUES (?, ?)";
        try (
        PreparedStatement pStmt = conn.prepareStatement(sqlInsert)) {
            pStmt.setString(1, Integer.toString(id)); 
            pStmt.setString(2, spezialitaet);
            pStmt.executeUpdate();
            System.out.println(System.lineSeparator()+"Dem Ganoven " + getName(id) + " wurde die Spezialität " + spezialitaet + " hinzugefügt");
            conn.close();        
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //neues Kopfgeld eines Ganoven wird in DB gespeichert
    public void kopfgeldHinzufuegen(int id, int kopfgeld ,String pdka) {
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlInsert = "INSERT INTO Kopfgeld (ID, Kopfgeld, PDKA) VALUES (?, ?, ?)";
        try (
        PreparedStatement pStmt = conn.prepareStatement(sqlInsert)) {
            pStmt.setString(1, Integer.toString(id)); 
            pStmt.setString(2, Integer.toString(kopfgeld));
            pStmt.setString(3, pdka);
            pStmt.executeUpdate();
            System.out.println(System.lineSeparator()+"Dem Ganoven " + getName(id) + " wurde ein Kopfgeld " + kopfgeld + " von " + pdka + " hinzugefügt");
            conn.close();        
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Gibt alle vergügbaren Daten zu einem Ganoven aus
    public void ganovenDatenSuchen(String id){
        Connection conn0 = connect();
        if (conn0==null) {
            return;
        }
        String sqlSelect0 = "SELECT * FROM Verbrecher WHERE ID LIKE ?";
        try(
        PreparedStatement pStmt = conn0.prepareStatement(sqlSelect0);      
        ) {
            pStmt.setString(1, id);    
            ResultSet rs = pStmt.executeQuery();   
            id = rs.getString("ID"); 
            String wohnort = rs.getString("Wohnort");
            String name = rs.getString("Name");
            System.out.println(System.lineSeparator()+"ID: "+id+", Name: "+name+", Wohnort: "+wohnort);
            rs.close();
            conn0.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }

        //Spezialitäten auslesen
        Connection conn1 = connect();
        if (conn1==null) {
            return;
        }
        String sqlSelect1 = "SELECT Spezialität FROM Spezialitäten WHERE ID = ?";
        try(
        PreparedStatement pStmt = conn1.prepareStatement(sqlSelect1);      
        ) {
            pStmt.setString(1, id);    
            ResultSet rs = pStmt.executeQuery(); 
            int n = 0;
            while (rs.next()) {
                n++;
                String spezialität = rs.getString("Spezialität");
                System.out.println("Spezialität "+n+": "+spezialität);
            }
            rs.close();
            conn1.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }

        //Kopfgelder auslesen
        Connection conn2 = connect();
        if (conn2==null) {
            return;
        }
        String sqlSelect2 = "SELECT Kopfgeld, PDKA FROM Kopfgeld WHERE ID = ?";
        try(
        PreparedStatement pStmt = conn2.prepareStatement(sqlSelect2);      
        ) {
            pStmt.setString(1, id);    
            ResultSet rs = pStmt.executeQuery(); 
            int n = 0;
            while (rs.next()) {
                n++;
                String kopfgeld = rs.getString("Kopfgeld");
                String PDKA = rs.getString("PDKA");
                System.out.println("Kopfgeld "+n+": "+kopfgeld+"€, PDKA: "+PDKA);
            }
            rs.close();
            conn2.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }

        //Haftzeiten auslesen
        Connection conn3 = connect();
        if (conn3==null) {
            return;
        }
        String sqlSelect3 = "SELECT Haftbeginn, Haftende FROM Haftzeit WHERE ID = ?";
        try(
        PreparedStatement pStmt = conn3.prepareStatement(sqlSelect3);     
        ) {
            pStmt.setString(1, id);    
            ResultSet rs = pStmt.executeQuery(); 
            int n = 0;
            while (rs.next()) {
                n++;
                int haftende = Integer.parseInt(rs.getString("Haftende"));
                int haftbeginn = Integer.parseInt(rs.getString("Haftbeginn"));
                System.out.println("Haft "+n+": "+datum(haftbeginn)+"(Haftbeginn),"+datum(haftende)+"(Haftende)");
            }
            rs.close();
            conn3.close();
        }
        catch (SQLException e) { 
            System.out.println(e.getMessage());
        }
    }

    //Gibt alle Einträge in einer Tabelle aus
    public void tabelleAusgeben(String tabelle){
        if (tabelle=="Haftzeit"){
            Connection conn = connect();
            if (conn==null) {
                return;
            }
            String sqlSelect = "SELECT * FROM Haftzeit";
            try(
            PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
            ) { 
                ResultSet rs = pStmt.executeQuery(); 
                int n = 0;
                while (rs.next()) {
                    n++;
                    String ID = rs.getString("ID");
                    int haftbeginn = Integer.parseInt(rs.getString("Haftbeginn"));
                    int haftende = Integer.parseInt(rs.getString("Haftende"));
                    System.out.println(System.lineSeparator()+"Haft "+n+": ");
                    System.out.println("ID: "+ID);
                    System.out.println("Haftbeginn: "+datum(haftbeginn));
                    System.out.println("Haftende: "+datum(haftende));
                }
                rs.close();
                conn.close();
            }
            catch (SQLException e) {        
                System.out.println(e.getMessage());
            }
        }else if (tabelle=="Kopfgeld"){
            Connection conn = connect();
            if (conn==null) {
                return;
            }
            String sqlSelect = "SELECT * FROM Kopfgeld";
            try(
            PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
            ) { 
                ResultSet rs = pStmt.executeQuery(); 
                int n = 0;
                while (rs.next()) {
                    n++;
                    String ID = rs.getString("ID");
                    String kopfgeld = rs.getString("Kopfgeld");
                    String PDKA = rs.getString("PDKA");
                    System.out.println(System.lineSeparator()+"Kopfgeld "+n+": ");
                    System.out.println("ID: "+ID);
                    System.out.println("Kopfgeld: "+kopfgeld+"€");
                    System.out.println("PDKA: "+PDKA);
                }
                rs.close();
                conn.close();
            }
            catch (SQLException e) {        
                System.out.println(e.getMessage());
            }

        }else if (tabelle=="Spezialitäten"){
            Connection conn = connect();
            if (conn==null) {
                return;
            }
            String sqlSelect = "SELECT * FROM Spezialitäten";
            try(
            PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
            ) { 
                ResultSet rs = pStmt.executeQuery(); 
                int n = 0;
                while (rs.next()) {
                    n++;
                    String ID = rs.getString("ID");
                    String spezialitaet = rs.getString("Spezialität");
                    System.out.println(System.lineSeparator()+"Spezialität "+n+": ");
                    System.out.println("ID: "+ID);
                    System.out.println("Spezialität: "+spezialitaet);
                }
                rs.close();
                conn.close();
            }
            catch (SQLException e) {        
                System.out.println(e.getMessage());
            }

        }else if (tabelle=="Verbrecher"){
            Connection conn = connect();
            if (conn==null) {
                return;
            }
            String sqlSelect = "SELECT * FROM Verbrecher";
            try(
            PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
            ) { 
                ResultSet rs = pStmt.executeQuery(); 
                int n = 0;
                while (rs.next()) {
                    n++;
                    String ID = rs.getString("ID");
                    String name = rs.getString("Name");
                    String wohnort = rs.getString("Wohnort");
                    System.out.println(System.lineSeparator()+"Ganove "+n+": ");
                    System.out.println("ID: "+ID);
                    System.out.println("Name: "+name);
                    System.out.println("Wohnort: "+wohnort);
                }
                rs.close();
                conn.close();
            }
            catch (SQLException e) {        
                System.out.println(e.getMessage());
            }

        }else{
            System.out.println("Diese Tabelle existiert nicht. Es existieren: Verbrecher, Spezialitäten, Kopfgeld, Haftzeit");
        }
    }

    //Gibt aus ob eine Ganove an einem bestimmten Tag in Haft war
    public boolean warInHaft(int id, int datum){
        Connection conn = connect();
        if (conn==null) {
            return false;
        }
        String sqlSelect = "SELECT Haftbeginn, Haftende FROM Haftzeit WHERE ID = ?";
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
        ) { 
            pStmt.setString(1, Integer.toString(id));
            ResultSet rs = pStmt.executeQuery(); 
            boolean tf = false;
            while (rs.next()) {
                int haftbeginn = Integer.parseInt(rs.getString("Haftbeginn"));
                int haftende = Integer.parseInt(rs.getString("Haftende"));
                if ((haftbeginn <= datum) && (haftende >= datum)){
                    tf = true;
                    System.out.println(System.lineSeparator()+"Ganove "+getName(id)+" war in Haft von "+datum(haftbeginn)+" bis "+datum(haftende)+".");
                }
            }
            rs.close();
            conn.close();
            return tf;
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
            return false;
        }
    }

    //Gibt alle Ganoven mit bestimmter Spezialität aus
    public void ganovenMitSpezialitaet(String spezialitaet){
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlSelect = "SELECT ID FROM Spezialitäten WHERE Spezialität = ?";
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlSelect);      
        ) { 
            pStmt.setString(1, spezialitaet);
            ResultSet rs = pStmt.executeQuery(); 
            while (rs.next()) {
                int id = Integer.parseInt(rs.getString("ID"));
                System.out.println(System.lineSeparator()+"Ganove "+getName(id)+" hat die Spezialität "+spezialitaet);
            }
            rs.close();
            conn.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }
    }
    
    //Ändert ausgewählten Haftzeit Eintrag in DB
    public void haftzeitAendern(String id, String haftbeginn, String haftende, String neuerHaftbeginn, String neuesHaftende){
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlUpdate = "UPDATE Haftzeit SET Haftbeginn = ?, Haftende = ? WHERE ID=? AND Haftbeginn=? AND Haftende=?";
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlUpdate);      
        ) { 
            pStmt.setString(1, neuerHaftbeginn);
            pStmt.setString(2, neuesHaftende);
            int neuererHaftbeginn = Integer.parseInt(neuerHaftbeginn);
            int neueresHaftende = Integer.parseInt(neuesHaftende);
            pStmt.setString(3, id);
            pStmt.setString(4, haftbeginn);
            pStmt.setString(5, haftende);
            pStmt.executeUpdate();
            System.out.println("Die Änderung wurde durchgeführt("+id+","+datum(neuererHaftbeginn)+","+datum(neueresHaftende)+")");
            conn.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }
    }
    
    //Ändert ausgewählten Kopfgeld Eintrag in DB
    public void kopfgeldAendern(String id, String kopfgeld, String pdka, String neuesKopfgeld, String neuPdka){
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlUpdate = "UPDATE Kopfgeld SET Kopfgeld = ?, PDKA = ? WHERE ID=? AND Kopfgeld=? AND PDKA=?";
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlUpdate);      
        ) { 
            pStmt.setString(1, neuesKopfgeld);
            pStmt.setString(2, neuPdka);
            pStmt.setString(3, id);
            pStmt.setString(4, kopfgeld);
            pStmt.setString(5, pdka);
            pStmt.executeUpdate();
            System.out.println("Die Änderung wurde durchgeführt("+id+","+neuesKopfgeld+","+neuPdka+")");
            conn.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }
    }
    
    //Ändert ausgewählten Wohnort Eintrag in DB
    public void wohnortAendern(String id, String neuerWohnort){
        Connection conn = connect();
        if (conn==null) {
            return;
        }
        String sqlUpdate = "UPDATE Verbrecher SET Wohnort = ? WHERE ID=?";
        try(
        PreparedStatement pStmt = conn.prepareStatement(sqlUpdate);      
        ) { 
            pStmt.setString(1, neuerWohnort);
            pStmt.setString(2, id);
            pStmt.executeUpdate();
            System.out.println("Die Änderung wurde durchgeführt("+id+","+neuerWohnort+")");
            conn.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }
    }
    
    //Gibt komplette DB aus
    public void kompletteListe(){
        int size = 0;
        Connection conn0 = connect();
        if (conn0==null) {
                return;
            }
        String sqlSelect0 = "SELECT count(*) FROM Verbrecher";
        try(
            PreparedStatement pStmt = conn0.prepareStatement(sqlSelect0);      
        ) {  
            ResultSet rs = pStmt.executeQuery();
            size = rs.getInt(1);
            rs.close();
            conn0.close();
        }
        catch (SQLException e) {        
            System.out.println(e.getMessage());
        }
        for(int i=0 ; i<size ; i++){  
            String id = Integer.toString(i);
            ganovenDatenSuchen(id);
        }    
    }
    
    //formatiert Datum
    public String datum(int datum) {
        String str = Integer.toString(datum);
        String newSub = ".";
        int index = 4;
        int index2 = 7;
        StringBuffer resString = new StringBuffer(str);
        resString.insert(index, newSub);
        resString.insert(index2, newSub);
        return resString.toString();
    }
    

}

