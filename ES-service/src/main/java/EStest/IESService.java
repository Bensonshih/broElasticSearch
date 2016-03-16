package EStest;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.json.JSONException;
import org.json.JSONObject;

public interface IESService {
	
	public JSONObject createIndex(String index,String type,JSONObject content) throws JSONException,IOException;
	
	public JSONObject getIndex(String index,String type,String id) throws JSONException;
	
    public JSONObject search(String index,String type, double  lat, double  lon,int page,String tag) throws JSONException;
    
    public JSONObject update(String index,String type,String id,String field,String[] value) throws JSONException, InterruptedException, ExecutionException, IOException;
    
    public JSONObject delete(String index,String type,String id) throws JSONException;
    

}
