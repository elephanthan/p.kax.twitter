package ssu.pickax.twitter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import twitter4j.Status;

public class MysqlHandler {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://118.131.253.37:13306/pickax";

	static final String USERNAME = "root";
	static final String PASSWORD = "Wkqqn12#$";

	Connection conn = null;
	PreparedStatement pstmt = null;

	private String[] keyword_arr;

	public String[] getKeyword_arr() {
		return this.keyword_arr;
	}

	public MysqlHandler() {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
			System.out.println("\n- MySQL Connection");
			String sql = "SELECT crawlerKeyword from crawlerInfo where crawlerSeq = 2";
			pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			String keywords = rs.getString("crawlerKeyword");
			this.keyword_arr = keywords.split(",");

			// rs.close();
			// stmt.close();
			// conn.close();
		} catch (SQLException se1) {
			se1.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void insertTweet(Status status) {
		String sql = "INSERT INTO twitter (tweet_id, tweet_keyword, tweet_user, tweet_text) VALUES(?,?,?,?)";
		String statusText = status.getText();
		for (String keyword : keyword_arr) {
			if(statusText.toLowerCase().contains(keyword.toLowerCase())){
				try {
					pstmt = conn.prepareStatement(sql);
					pstmt.setLong(1, status.getId());
					pstmt.setString(2, keyword);
					pstmt.setString(3, status.getUser().getScreenName());
					pstmt.setString(4, statusText);
					pstmt.executeUpdate();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("########ERROR QUERY########");
					System.out.println(pstmt);
				}
			}
		}
		
	}

}
