package EStest;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;



@Service
public class ESService implements IESService {
	
//	private  Node node;
//	private  Client client;
	private Settings settings;
	private TransportClient client;
	private final String serverIP = "45.63.127.121";
	private final int elasticSearchPort = 9963;

	//form a json document
	public static Map<String, Object> putJsonDocument(String[] tags, String device_id, String device_type,
            JSONObject location,long point,String sns_token){

	Map<String, Object> jsonDocument = new HashMap<String, Object>();
	
		jsonDocument.put("tag", tags);
		jsonDocument.put("device_id", device_id);
		jsonDocument.put("device_type", device_type);
		jsonDocument.put("location", location);
		jsonDocument.put("point", point);
		jsonDocument.put("sns_token", sns_token);
		
		return jsonDocument;
	}
	
	@PostConstruct
	public void init() throws UnknownHostException{
//		node = nodeBuilder().node();
//		client = node.client();
		settings = Settings.settingsBuilder()
		        .put("cluster.name", "elasticsearch")
		        .put("client.transport.sniff",false)
		        .put("client.transport.ping_timeout", 20, TimeUnit.SECONDS)
		        .build();
		client = TransportClient.builder().settings(settings).build();
		
		client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(serverIP), elasticSearchPort));
	}

	@Override
	public JSONObject createIndex(String index, String type,JSONObject content) throws JSONException, IOException {
		JSONObject resp = new JSONObject();
		
		/*client.prepareIndex(index, type,id)
        .setSource(putJsonDocument(content.getString("name"),
        							content.getInt("age"),
        							content.getString("gender"),
        							content.getString("message"))).execute().actionGet();
		
		node.close();*/
		String postString="{\n"
				+ "\"device\":{"
				+ "\"properties\":{"
				+ "\"device_id\" : \""+ content.getString("device_id") +"\",\n"
				+ "\"tag\" :"+ content.getJSONArray("tag") +",\n"
				+ "\"device_type\" : \""+ content.getString("device_type") +"\",\n"
				+ "\"point\" :"+ content.getLong("point") +",\n"
				+ "\"sns_token\" : \""+ content.getString("sns_token") +"\",\n"
				+ "\"location\" : "+ content.getJSONArray("location") +"\n"
				+ "}\n" //properties
				+ "}\n" //device
				+ "}";  
		System.out.println("post string: " + postString);
		System.out.println("Created/Updated tag: " + content.getJSONArray("tag"));
		/*IndexResponse response = client.prepareIndex(index, type, content.getString("device_id"))
		        .setSource(jsonBuilder()
		                    .startObject()
		                        .field("tag", content.getJSONArray("tag"))
		                        .field("device_id", content.getString("device_id"))
		                        .field("device_type", content.get("device_type"))
		                        .field("location", content.getJSONObject("location"))
		                        .field("point", content.getLong("point"))
		                        .field("sns_token", content.getString("sns_token"))
		                    .endObject()
		                  )
		        .get();*/
		IndexResponse response = client.prepareIndex(index, type, content.getString("device_id"))
		        .setSource(postString)
		        .get();
		boolean isCreated = response.isCreated();
		if(isCreated){
			resp.put("message", "Create success");
		}else{
			resp.put("message", "Update success");
		}
		resp.put("statuscode", 200);
		
		//node.close();
		//client.close();
		
		return resp;
	}

	@Override
	public JSONObject getIndex(String index, String type, String device_id) {
		JSONObject resp = null;
//		Node node  = NodeBuilder.nodeBuilder().build().start();
//		Client client = node.client();
//		GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();
		GetResponse response = client.prepareGet(index, type, device_id).get();
		
		resp = new JSONObject();
		Map<String, Object> source = response.getSource();
		System.out.println("result: " + source);
		if (source != null) {
			resp.put("Index", response.getIndex());
			resp.put("Type", response.getType());
			resp.put("Id", response.getId());
			resp.put("Version", response.getVersion());
			resp.put("Source", source);
			resp.put("statuscode", 200);
		}else{
			resp.put("statuscode", 500);
			resp.put("message", "Index not found");
		}
		
		
		//node.close();
		//client.close();
		
		return resp;
	
	}

	@Override
	public JSONObject search(String index, String type, double  lat, double  lon,int page,String tag) {
		JSONObject resp = new JSONObject();
		JSONArray resultArray = new JSONArray();
		int size = 10;
//		Node node  = NodeBuilder.nodeBuilder().build().start();
//		Client client = node.client();
//		SearchResponse response = client.prepareSearch(index)
//	              .setTypes(type)
//	              .setSearchType(SearchType.QUERY_AND_FETCH)
//	              .setQuery(QueryBuilders.termQuery(field, value))
//	              .setFrom(0).setSize(60).setExplain(true)
//	              .execute()
//	              .actionGet();
		
		  
		
		SearchResponse response = client.prepareSearch(index)
		        .setTypes(type)
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setFetchSource(new String[]{"device_id"}, null)
		        .addSort(SortBuilders.geoDistanceSort("location")
		        		.order(SortOrder.ASC)
		        		.point(lat, lon)
		                .unit(DistanceUnit.KILOMETERS))
		        .setQuery(QueryBuilders.matchQuery("tag", tag)) // Query
		        //.setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder))
		        .setPostFilter(QueryBuilders.geoDistanceQuery("location").point(lat, lon).distance(200, DistanceUnit.KILOMETERS).optimizeBbox("memory").geoDistance(GeoDistance.ARC))     // Filter
		        .setFrom((page-1)*size).setSize(size).setExplain(true)
		        .execute()
		        .actionGet();
		
		/*String termQuery="{\n" 
						+ "  \"size\" : 1,\n"
						+ "\"fields\" : [\"device_id\"],\n"
						+ "\"sort\" : [{\"_geo_distance\":{\"location\": { \"lat\": 52.058, \"lon\": 121.563 }, \"order\": \"asc\",\"unit\": \"km\" }}],\n"
						+ " \"query\" : {\"filtered\": { \"query\": {"
						+ "    \"match\" : {\"tag\": \"くるま\"}\n" 
						+ "  },\n"
						+ " \"filter\": { \"geo_distance\":"
						+ "{\"distance\": \"3002km\", \"location\": {\"lat\": 52.058, \"lon\": 121.563}}\n"
						+ "}\n"
						+ "}\n"
						+ "}";*/
		
		SearchHit[] results = response.getHits().getHits();
		
		//System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			
			Map<String,Object> result = hit.getSource();
			resultArray.put(result);
		}
		
		
		resp.put("statuscode", 200);
		resp.put("result", resultArray);
		//node.close();
		//client.close();
		return resp;
	}

	@Override
	public JSONObject update(String index, String type, String id, String field, String[] value) throws JSONException, InterruptedException, ExecutionException, IOException {
		JSONObject resp = new JSONObject();
//		Node node  = NodeBuilder.nodeBuilder().build().start();
//		Client client = node.client();
		
		Map<String, Object> updateObject = new HashMap<String, Object>();
        updateObject.put(field, value);

        //client.prepareUpdate(index, type, id)
       //       .setScript("ctx._source." + field + "=" + field)
       //       .setScriptParams(updateObject).execute().actionGet();
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                .startObject()
                    .field(field, value)
                .endObject());
        client.update(updateRequest).get();
        
       // node.close();
       // client.close();
        
		return resp.put("statuscode", 200);
	}

	@Override
	public JSONObject delete(String index, String type, String id) throws JSONException {
		JSONObject resp = new JSONObject();
//		Node node  = NodeBuilder.nodeBuilder().build().start();
//		Client client = node.client();
		DeleteResponse response = client.prepareDelete(index, type, id).get();
		
		resp = new JSONObject();
		
		resp.put("Index", response.getIndex());
		resp.put("Type", response.getType());
		resp.put("Id", response.getId());
		resp.put("Version", response.getVersion());
		resp.put("statuscode", 200);
		//node.close();
		//client.close();
		
		return resp;
	}

	
	

}
