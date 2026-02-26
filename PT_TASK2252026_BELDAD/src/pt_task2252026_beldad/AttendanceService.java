package pt_task2252026_beldad;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AttendanceService {

    private static final String DB_URL = "jdbc:derby://localhost:1527/SigmaBaseV1";
    private static final String DB_USER = "beldad";
    private static final String DB_PASSWORD = "app";

    public AttendanceService() {
        createAttendanceTableIfMissing();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    private void createAttendanceTableIfMissing() {
        String sql = "CREATE TABLE ATTENDANCE ("
                + "STUDENT_ID VARCHAR(50) NOT NULL, "
                + "NAME VARCHAR(100) NOT NULL, "
                + "\"DATE\" DATE NOT NULL, "
                + "STATUS VARCHAR(20) NOT NULL, "
                + "PRIMARY KEY (STUDENT_ID, \"DATE\"))";

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.execute();
        } catch (SQLException ex) {
            if (!Objects.equals(ex.getSQLState(), "X0Y32")) {
                throw new RuntimeException("Unable to initialize ATTENDANCE table: " + ex.getMessage(), ex);
            }
        }
    }

    public boolean attendanceExists(String studentId, Date attendanceDate) throws SQLException {
        String sql = "SELECT 1 FROM ATTENDANCE WHERE STUDENT_ID = ? AND \"DATE\" = ?";

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, studentId);
            pst.setDate(2, attendanceDate);

            try (ResultSet rs = pst.executeQuery()) {
                return rs.next();
            }
        }
    }

    public void addAttendance(String studentId, String name, Date attendanceDate, String status) throws SQLException {
        String sql = "INSERT INTO ATTENDANCE (STUDENT_ID, NAME, \"DATE\", STATUS) VALUES (?, ?, ?, ?)";

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, studentId);
            pst.setString(2, name);
            pst.setDate(3, attendanceDate);
            pst.setString(4, status);
            pst.executeUpdate();
        }
    }

    public int updateAttendance(String studentId, String name, Date attendanceDate, String status) throws SQLException {
        String sql = "UPDATE ATTENDANCE SET NAME = ?, STATUS = ? WHERE STUDENT_ID = ? AND \"DATE\" = ?";

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, name);
            pst.setString(2, status);
            pst.setString(3, studentId);
            pst.setDate(4, attendanceDate);
            return pst.executeUpdate();
        }
    }

    public int deleteAttendance(String studentId, Date attendanceDate) throws SQLException {
        String sql = "DELETE FROM ATTENDANCE WHERE STUDENT_ID = ? AND \"DATE\" = ?";

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {
            pst.setString(1, studentId);
            pst.setDate(2, attendanceDate);
            return pst.executeUpdate();
        }
    }

    public List<Object[]> getAttendanceRecords(Date filterDate) throws SQLException {
        String sql;
        if (filterDate == null) {
            sql = "SELECT STUDENT_ID, NAME, \"DATE\", STATUS FROM ATTENDANCE ORDER BY \"DATE\" DESC, STUDENT_ID";
        } else {
            sql = "SELECT STUDENT_ID, NAME, \"DATE\", STATUS FROM ATTENDANCE WHERE \"DATE\" = ? ORDER BY STUDENT_ID";
        }

        try (Connection con = getConnection();
             PreparedStatement pst = con.prepareStatement(sql)) {

            if (filterDate != null) {
                pst.setDate(1, filterDate);
            }

            List<Object[]> rows = new ArrayList<>();
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    rows.add(new Object[]{
                        rs.getString("STUDENT_ID"),
                        rs.getString("NAME"),
                        rs.getDate("DATE"),
                        rs.getString("STATUS")
                    });
                }
            }
            return rows;
        }
    }
}
