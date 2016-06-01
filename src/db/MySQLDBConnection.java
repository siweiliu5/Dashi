package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import yelp.YelpAPI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import model.Restaurant;

import org.json.JSONArray;
import org.json.JSONObject;

import yelp.YelpAPI;

public class MySQLDBConnection implements DBConnection {
	// May ask for implementation of other methods. Just add empty body to them.
	private Connection conn;
	private static final int MAX_RECOMMENDED_RESTAURANTS = 20;

	public MySQLDBConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn = DriverManager.getConnection(DBUtil.URL);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public JSONArray searchRestaurants(String userId, double lat, double lon) {
		try {
			YelpAPI api = new YelpAPI();
			// Visit YelpAPI to get nearby restaurants
			JSONObject response = new JSONObject(api.searchForBusinessesByLocation(lat, lon));
			JSONArray array = (JSONArray) response.get("businesses");
			// Visit Database to get historical visited restaurants
			Set<String> visited = getVisitedRestaurants(userId);
			
			List<JSONObject> list = new ArrayList<>();

			for (int i = 0; i < array.length(); i++) {
				/*
				 * JSONObject object = array.getJSONObject(i); Restaurant
				 * restaurant = new Restaurant(object); JSONObject obj =
				 * restaurant.toJSONObject(); list.add(obj)
				 */
				JSONObject object = array.getJSONObject(i);
				Restaurant restaurant = new Restaurant(object);
				String businessId = restaurant.getBusinessId();
				String name = restaurant.getName();
				String categories = restaurant.getCategories();
				String city = restaurant.getCity();
				String state = restaurant.getState();
				String fullAddress = restaurant.getFullAddress();
				double stars = restaurant.getStars();
				double latitude = restaurant.getLatitude();
				double longitude = restaurant.getLongitude();
				String imageUrl = restaurant.getImageUrl();
				String url = restaurant.getUrl();
				JSONObject obj = restaurant.toJSONObject();
				if (visited.contains(businessId)) {
					obj.put("is_visited", true);
				} else {
					obj.put("is_visited", false);
				}
				executeUpdateStatement("INSERT IGNORE INTO restaurants " + "VALUES ('" + businessId + "', \"" + name
						+ "\", \"" + categories + "\", \"" + city + "\", \"" + state + "\", " + stars + ", \""
						+ fullAddress + "\", " + latitude + "," + longitude + ",\"" + imageUrl + "\", \"" + url
						+ "\")");
				list.add(obj);
			}
			return new JSONArray(list);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private boolean executeUpdateStatement(String query) {
		if (conn == null) {
			return false;
		}
		try {
			Statement stmt = conn.createStatement();
			System.out.println("\nDBConnection executing query:\n" + query);
			stmt.executeUpdate(query);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	// Return a set named 'ResultSet' to contain returned results
	private ResultSet executeFetchStatement(String query) {
		if (conn == null) {
			return null;
		}
		try {
			Statement stmt = conn.createStatement();
			System.out.println("\nDBConnection executing query:\n" + query);
			return stmt.executeQuery(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean setVisitedRestaurants(String userId, List<String> businessIds) {
		boolean result = true;
		for (String businessId : businessIds) {
			// INSERT INTO history (`user_id`, `business_id`) VALUES ("1111",
			// "abcd");
			if (!executeUpdateStatement("INSERT INTO history (`user_id`, `business_id`) VALUES (\"" + userId + "\", \""
					+ businessId + "\")")) {
						result = false;
					}
		}
		return result;
	}

	@Override
	public void unsetVisitedRestaurants(String userId, List<String> businessIds) {
		for (String businessId : businessIds) {
			executeUpdateStatement("DELETE FROM history WHERE `user_id`=\"" + userId + "\" and `business_id` = \""
					+ businessId + "\"");
		}
	}

	@Override
	public Set<String> getVisitedRestaurants(String userId) {
		Set<String> visitedRestaurants = new HashSet<String>();
		try {
			String sql = "SELECT business_id from history WHERE user_id=" + userId;
			ResultSet rs = executeFetchStatement(sql);
			while (rs.next()) {
				String visitedRestaurant = rs.getString("business_id");
				visitedRestaurants.add(visitedRestaurant);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return visitedRestaurants;
	}

	@Override
	public JSONObject getRestaurantsById(String businessId, boolean isVisited) {
		try {
			String sql = "SELECT * from " + "restaurants where business_id='" + businessId + "'"
					+ " ORDER BY stars DESC";
			ResultSet rs = executeFetchStatement(sql);
			if (rs.next()) {
				Restaurant restaurant = new Restaurant(
						rs.getString("business_id"), 
						rs.getString("name"),
						rs.getString("categories"), 
						rs.getString("city"), 
						rs.getString("state"),
						rs.getString("full_address"), 
						rs.getFloat("stars"), 
						rs.getFloat("latitude"),
						rs.getFloat("longitude"), 
						rs.getString("image_url"), 
						rs.getString("url"));
				JSONObject obj = restaurant.toJSONObject();
				obj.put("is_visited", isVisited);
				return obj;
			}
		} catch (Exception e) { /* report an error */
			System.out.println(e.getMessage());
		}
		return null;
	}

	@Override
	public JSONArray recommendRestaurants(String userId) {
		try {
			// step 1, visit Database 'history' by userID to get restaurants BusinessID
			Set<String> visitedRestaurants = getVisitedRestaurants(userId);
			// step 2, visit Database 'restaurants' by BusiniessID to get Categories
			// hash set: eliminate duplicate
			Set<String> allCategories = new HashSet<>();
			for (String restaurant : visitedRestaurants) {
				allCategories.addAll(getCategories(restaurant));
			}
			// step 3, Visit Database 'restaurants' by Categories like to get BusinessID
			Set<String> allRestaurants = new HashSet<>();
			for (String category : allCategories) {
				Set<String> set = getBusinessId(category);
				allRestaurants.addAll(set);
			}
			// step 4, Visit Database 'restaurants' by Categories like to get JSONObject
			Set<JSONObject> diff = new HashSet<>();
			int count = 0;
			for (String businessId : allRestaurants) {
				// Perform filtering
				if (!visitedRestaurants.contains(businessId)) {
					diff.add(getRestaurantsById(businessId, false));
					count++;
					if (count >= MAX_RECOMMENDED_RESTAURANTS) {
						break;
					}
				}
			}
			return new JSONArray(diff);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}

	@Override
	// Visit DataBase by businessID to get categories
	public Set<String> getCategories(String businessId) {
		try {
			String sql = "SELECT categories from restaurants WHERE business_id='" + businessId + "'";
			ResultSet rs = executeFetchStatement(sql);
			if (rs.next()) {
				Set<String> set = new HashSet<>();
				String[] categories = rs.getString("categories").split(",");
				for (String category : categories) {
					// ' Japanese ' -> 'Japanese'
					set.add(category.trim());
				}
				return set;
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return new HashSet<String>();
	}

	@Override
	public Set<String> getBusinessId(String category) {
		// TODO Auto-generated method stub
		try {
			Set<String> set = new HashSet<>();
			String sql = "SELECT business_id from restaurants WHERE categories LIKE'%" + category + "%'";
			ResultSet rs = executeFetchStatement(sql);
			while (rs.next()) {
				String id = rs.getString("business_id");
				set.add(id);
			}
			return set;
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return new HashSet<String>();
	}

	@Override
	public Boolean verifyLogin(String userId, String password) { // password is
																	// hashed
		try {
			if (conn == null) {
				return false;
			}
			//String sql = "SELECT user_id from users WHERE user_id='" + userId + "' and password='" + password + "'";
			//ResultSet rs = executeFetchStatement(sql);
			String sql = "SELECT user_id from users WHERE user_id=? and password=?";
			PreparedStatement pstmt = conn.prepareStatement( sql );
			pstmt.setString( 1, userId); 
			pstmt.setString( 2, password); 
			ResultSet rs = pstmt.executeQuery();
			
			if (rs.next()) {
				return true;
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return false;
	}

	@Override
	public String getFirstLastName(String userId) {
		String name = "";
		try {
			if (conn != null) {
				String sql = "SELECT first_name, last_name from users WHERE user_id='" + userId + "'";
				ResultSet rs = executeFetchStatement(sql);
				if (rs.next()) {
					name += rs.getString("first_name") + " " + rs.getString("last_name");
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return name;
	}
}
