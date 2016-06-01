package api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import db.DBConnection;
import db.MySQLDBConnection;

/**
 * Servlet implementation class VisitHistory
 */
@WebServlet({ "/VisitHistory", "/history" })
public class VisitHistory extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public VisitHistory() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		//response.getWriter().append("Served at: ").append(request.getContextPath());
		// allow access only if session exists
		HttpSession session = request.getSession();
		if (session.getAttribute("user") == null) {
			response.setStatus(403);
			return;
		}
		try {
			DBConnection connection = new MySQLDBConnection();
			JSONArray array = null;
			if (request.getParameterMap().containsKey("user_id")) {
				String userId = request.getParameter("user_id");
				Set<String> mySet = connection.getVisitedRestaurants(userId);
				array = new JSONArray();
				for (String businessId : mySet) {
					array.put(connection.getRestaurantsById(businessId, true));
				}
				RpcParser.writeOutput(response, array);
				RpcParser.writeOutput(response, new JSONObject().put("status", "OK"));
			} else {
				RpcParser.writeOutput(response, new JSONObject().put("status", "InvalidParameter"));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	public void doDelete (HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		try {
			DBConnection connection = new MySQLDBConnection();
			JSONObject input = RpcParser.parseInput(request);
			if (input.has("user_id") && input.has("visited")) {
				String userId = (String) input.get("user_id");
				JSONArray array = (JSONArray) input.get("visited");
				List<String> list = new ArrayList<>();
				for (int i = 0; i < array.length(); i++) {
					String businessId = (String)array.get(i);
					list.add(businessId);
				}
				connection.unsetVisitedRestaurants(userId, list);
				RpcParser.writeOutput(response, new JSONObject().put("status", "OK"));
			} else {
				RpcParser.writeOutput(response, new JSONObject().put("status", "InvalidParameter"));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		// doGet(request, response);
		try {
			DBConnection connection = new MySQLDBConnection();
			JSONObject input = RpcParser.parseInput(request);
			if (input.has("user_id") && input.has("visited")) {
				String userId = (String) input.get("user_id");
				JSONArray array = (JSONArray) input.get("visited");
				List<String> visitedRestaurants = new ArrayList<>();
				for (int i = 0; i < array.length(); i++) {
					String businessId = (String) array.get(i);
					visitedRestaurants.add(businessId);
				}
				/*
				 * connection.setVisitedRestaurants(userId, visitedRestaurants);
				 * RpcParser.writeOutput(response, new
				 * JSONObject().put("status", "OK"));
				 */
				if (connection.setVisitedRestaurants(userId, visitedRestaurants)) {
					RpcParser.writeOutput(response, new JSONObject().put("status", "OK"));
				} else {
					RpcParser.writeOutput(response, new JSONObject().put("status", "InvalidParameter"));
				}
			} else {
				RpcParser.writeOutput(response, new JSONObject().put("status", "InvalidParameter"));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
