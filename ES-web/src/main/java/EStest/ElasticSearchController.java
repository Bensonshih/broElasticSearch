package EStest;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class ElasticSearchController {
	
	
	
	@Autowired
	//@Qualifier("")
	IESService service;
	
	@RequestMapping(value = "/{index}/{type}", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE,produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String createIndex(@PathVariable("index") String index,@PathVariable("type") String type,@RequestBody String requestBody) {
		JSONObject resp = new JSONObject();
		JSONObject reqBody = new JSONObject(requestBody);
		
		try {
			if (index == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"index\" not found");
			}
			if (type == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"type\" not found");
			}
		
		
			resp = service.createIndex(index, type, reqBody);
		} catch (JSONException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		} catch (IOException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		}
		
		return resp.toString();

	}
	
	@RequestMapping(value = "/{index}/{type}/{id}", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE,produces = MediaType.APPLICATION_JSON_VALUE + ";charset=utf-8")
	@ResponseBody
	public String getIndex(@PathVariable("index") String index,@PathVariable("type") String type,@PathVariable("id") String id) {
		JSONObject resp = null;
		try {
			if (index == null) {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"index\" not found");
			}
			if (type == null) {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"type\" not found");
			}
			if (id == null || id == "") {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"id\" not found");
			}
				
				resp = service.getIndex(index, type, id);
				
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			resp = new JSONObject();
			resp.put("statuscode", 500);
			resp.put("message", e.getMessage());
			return resp.toString();
		} finally{
			//this.service.shutdown();
		}
		return resp.toString();
	}
	
	@RequestMapping(value = "/{index}/{type}/search", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE + ";charset=utf-8")
	@ResponseBody
	public String search(@PathVariable("index") String index,@PathVariable("type") String type,@RequestBody String requestBody) {
		JSONObject resp = new JSONObject();
		JSONObject reqBody = new JSONObject(requestBody);
		String tag = null;
		double lon = 0,lat = 0;
		int page = 0;
		
		try {
			if (index == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"index\" not found");
			}
			if (type == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"type\" not found");
			}
			if (reqBody.getString("tag") != null) {
				tag = reqBody.getString("tag");
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"tag\" not found");
			}
			if (reqBody.has("lat")) {
				lat = reqBody.getDouble("lat");
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"lat\" not found");
			}
			if (reqBody.has("lon")) {
				lon = reqBody.getDouble("lon");
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"lon\" not found");
			}
			if (reqBody.has("page")) {
				page = reqBody.getInt("page");
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"page\" not found");
			}
		
			resp = service.search(index, type, lat, lon, page, tag);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		}
		
		return resp.toString();

	}
	
	@RequestMapping(value = "/{index}/{type}/{id}/update", method = RequestMethod.PUT, consumes = MediaType.APPLICATION_JSON_VALUE,produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String update(@PathVariable("index") String index,@PathVariable("type") String type,@PathVariable("id") String id,@RequestBody String requestBody) {
		JSONObject resp = new JSONObject();
		JSONObject reqBody = new JSONObject(requestBody);
		String field = null;
		String[] value = null;
		
		try {
			if (index == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"index\" not found");
			}
			if (type == null) {
				resp.put("statuscode", 400);
				throw new JSONException("\"type\" not found");
			}
			if (reqBody.getString("field") != null) {
				field = reqBody.getString("field");
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"field\" not found");
			}
			if (reqBody.get("value") != null) {
				JSONArray jsonArray = reqBody.getJSONArray("value");
				value = new String[jsonArray.length()];
				for (int i = 0; i < value.length; i++) {
					value[i] = jsonArray.getString(i);
				}
				
			}else{
				resp.put("statuscode", 400);
				throw new JSONException("\"vaule\" not found");
			}
		
			resp = service.update(index, type, id, field, value);
		} catch (JSONException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		} catch (InterruptedException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		} catch (ExecutionException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		} catch (IOException e) {
			e.printStackTrace();
			resp.put("message", e.getMessage());
			resp.put("statuscode", 500);
			return resp.toString();
		}
		
		return resp.toString();

	}
	
	@RequestMapping(value = "/{index}/{type}/{id}", method = RequestMethod.DELETE, consumes = MediaType.APPLICATION_JSON_VALUE,produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String delete(@PathVariable("index") String index,@PathVariable("type") String type,@PathVariable("id") String id) {
		JSONObject resp = null;
		try {
			if (index == null) {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"index\" not found");
			}
			if (type == null) {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"type\" not found");
			}
			if (id == null || id == "") {
				resp = new JSONObject();
				resp.put("statuscode", 400);
				throw new JSONException("\"id\" not found");
			}
				
				resp = service.delete(index, type, id);
				
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			resp = new JSONObject();
			resp.put("statuscode", 500);
			resp.put("message", e.getMessage());
			return resp.toString();
		} finally{
			//this.service.shutdown();
		}
		return resp.toString();
	}

	
}
