package api;

import java.io.IOException;
import java.io.PrintWriter;

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
 * Servlet implementation class RecommendRestaurants
 */
@WebServlet({ "/RecommendRestaurants", "/recommendation" })
public class RecommendRestaurants extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public RecommendRestaurants() {
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
		// response.getWriter().append("Served at:
		// ").append(request.getContextPath());
		// ------------First time not using RpcParser helper class
		// -------------------
		response.setContentType("application/json");
		response.addHeader("Access-Control-Allow-Origin", "*");
		/*
		try {	
			if (request.getParameter("user_id") != null) {
				JSONArray jarray = new JSONArray();
				JSONObject jobj1 = new JSONObject();
				JSONObject jobj2 = new JSONObject();
				jobj1.put("country", "united states").put("location", "downtown").put("name", "panda express");

				jobj2.put("name", "hongkong express").put("location", "uptown").put("country", "united states");

				jarray.put(jobj1);
				jarray.put(jobj2);
				// output values
				PrintWriter out = response.getWriter();
				out.print(jarray);
				out.flush();
				out.close();
			}
			*/
		// allow access only if session exists
			
			HttpSession session = request.getSession();
			if (session.getAttribute("user") == null) {
				response.setStatus(403);
				return;
			}
			
			DBConnection connection = new MySQLDBConnection();
			JSONArray array = new JSONArray();
			if (request.getParameterMap().containsKey("user_id")) {
				String userId = request.getParameter("user_id");
				array = connection.recommendRestaurants(userId);
			}
			RpcParser.writeOutput(response, array);
			//connection.close();
		/*
		} catch (JSONException e) {
			e.printStackTrace();
		}
		*/
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
