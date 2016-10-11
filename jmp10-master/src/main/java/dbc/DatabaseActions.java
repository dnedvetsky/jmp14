package dbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dmitry on 13.09.2016.
 */
public class DatabaseActions {
    private final DataBaseDriver databaseDriver = new DataBaseDriver();
    private List<String> usersBatch = new ArrayList<>();

    private final String CREATEUSERSTABLE = "CREATE TABLE IF NOT EXISTS Users (" +
            " ID INTEGER PRIMARY KEY AUTOINCREMENT," +
            " Name TEXT NOT NULL," +
            " Surname TEXT NOT NULL," +
            " Birthdate TEXT);";
    private final String CREATEFRIENDSHIPTABLE = "CREATE TABLE IF NOT EXISTS Friends (" +
            " ID1 INTEGER NOT NULL," +
            " ID2 INTEGER NOT NULL," +
            " TimeStamp TEXT NOT NULL);";
    private final String CREATEPOSTS = "CREATE TABLE IF NOT EXISTS Posts (" +
            " ID INTEGER PRIMARY KEY AUTOINCREMENT," +
            " UserID INTEGER NOT NULL," +
            " Text TEXT," +
            " TimeStamp TEXT NOT NULL);";
    private final String CREATELIKES = "CREATE TABLE IF NOT EXISTS Likes (" +
            " ID INTEGER PRIMARY KEY AUTOINCREMENT," +
            " PostID INTEGER NOT NULL," +
            " UserID INTEGER NOT NULL," +
            " TimeStamp TEXT NOT NULL);";

    private final String ADDUSERSTOUSERSQUERY = "INSERT INTO Users (Name, Surname, Birthdate)" +
            "VALUES (?, ?, ?);";
    private final String ADDFRIENDSHIPS = "INSERT INTO Friends (ID1, ID2, TimeStamp)" +
            "VALUES (?, ?, ?);";
    private final String ADDPOSTSQUERY = "INSERT INTO Posts (UserID, Text, TimeStamp)" +
            "VALUES (?, ?, ?);";
    private final String ADDLIKESQUERY = "INSERT INTO Likes (PostID, UserID, TimeStamp)" +
            "VALUES (?, ?, ?);";

    public void addUsers(String name, String surname, String birthdate) {
        databaseDriver.executeStatement(ADDUSERSTOUSERSQUERY, name, surname, birthdate);
    }

    public void addFriendships(String userID1, String userID2, String timestamp) {
        databaseDriver.executeStatement(ADDFRIENDSHIPS, userID1, userID2, timestamp);
    }

    public void addPosts(String userID, String text, String timestamp) {
        databaseDriver.executeStatement(CREATEPOSTS, userID, text, timestamp);
    }

    public void dropTable(String table) {
        databaseDriver.tableActions("DROP TABLE IF EXISTS " + table + ";");
        System.out.println("Dropped datatable: " + table);
    }

    public void executePostsBatch(List<List<String>> lists) {
        databaseDriver.executeBatchStatement(ADDPOSTSQUERY, lists);
    }

    public void executeUsersBatch(List<List<String>> lists) {
        databaseDriver.executeBatchStatement(ADDUSERSTOUSERSQUERY, lists);
    }

    public void printNames(int likesCount, int friendsCount) {
        String sqlQuery = "SELECT Users.Name, Users.Surname, Users.Birthdate FROM Users WHERE Users.ID IN " +
                "(SELECT ID1 FROM Friends GROUP BY Friends.ID2 HAVING COUNT(ID2) > %d)" +
                "AND Users.ID IN " +
                "(SELECT Likes.UserID FROM Likes GROUP BY Likes.UserID HAVING COUNT(Likes.PostID)>%d) " +
                "AND Users.Birthdate BETWEEN '1998-12-11' AND '2002-12-11';";
        sqlQuery = String.format(sqlQuery, likesCount, friendsCount);
        databaseDriver.printSpecifiedFieldFromQuery(sqlQuery, "Name", "Surname", "Birthdate");
    }

    public void executeFriendsBatch(List<List<String>> lists) {
        databaseDriver.executeBatchStatement(ADDFRIENDSHIPS, lists);
    }

    public void createUsersTable() {
        databaseDriver.tableActions(CREATEUSERSTABLE);
    }

    public void createLikesTable() {
        databaseDriver.tableActions(CREATELIKES);
    }

    public void createFriendshipsTable() {
        databaseDriver.tableActions(CREATEFRIENDSHIPTABLE);
    }

    public int getLatestID(String tableName) {
        return databaseDriver.getLatestID(tableName);
    }

    public void createPostsTable() {
        databaseDriver.tableActions(CREATEPOSTS);
    }

    public void executeLikesBatch(List<List<String>> list) {
        databaseDriver.executeBatchStatement(ADDLIKESQUERY, list);
    }
}
