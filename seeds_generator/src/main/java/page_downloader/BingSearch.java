import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.StringReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.ArrayList;
import org.apache.commons.codec.binary.Base64;
import org.xml.sax.InputSource;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONString;

import java.net.URI;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;

public class BingSearch {
    
    private String accountKey;
    private Properties prop; 

    public BingSearch(){
	try{
	    prop = new Properties();
	    FileInputStream is = new FileInputStream("conf/config.properties");
	    prop.load(is);
	    accountKey = prop.getProperty("ACCOUNTKEY_BING");
	}   
	catch(Exception e){
	    e.printStackTrace();
	    prop = null;
	}
    } 

	
    public ArrayList<String> search(String query, String start, String top, String es_index, String es_doc_type, String es_server){
	if (this.prop == null){
	    System.err.println("Error: config file is not loaded yet");
	    return null;
	}
        int nTop = Integer.valueOf(top);
	int nStart = Integer.valueOf(start);

	String subquery = null;
	ArrayList<String> tag = null;

	Download download = new Download(query, subquery, tag, es_index, es_doc_type, es_server);
	ArrayList<String> results = new ArrayList<String>();
	    
	try {
	    int step = 10; //Bing can return maximum 50 results per query
	    URIBuilder builder = new URIBuilder("https://api.cognitive.microsoft.com/bing/v5.0/search");
	    builder.setParameter("q", query);
	    builder.setParameter("count", String.valueOf(step));
	    builder.setParameter("mkt", "en-us");
	    builder.setParameter("safesearch", "Off"); // allow results to include adult content
	    HttpClient httpclient = HttpClients.createDefault();
		
	    for (; nStart < nTop; nStart += step){
                builder.setParameter("offset", String.valueOf(nStart));
                URI uri = builder.build();

                HttpGet request = new HttpGet(uri);
                request.setHeader("Ocp-Apim-Subscription-Key", this.accountKey);
		
                HttpResponse response = httpclient.execute(request);
                HttpEntity entity = response.getEntity();

                String json_string = EntityUtils.toString(entity);
                JSONObject jsResponse = new JSONObject(json_string);
		if(jsResponse.has("webPages")){
		    JSONObject webPagesTemp = jsResponse.getJSONObject("webPages");
		    
		    JSONArray webpages = webPagesTemp.getJSONArray("value");
		    
		    for (int i=0; i<webpages.length(); i++){
			JSONObject item = webpages.getJSONObject(i);
			String url = (String)item.get("url");
            try {
                //Bing Search v5 returns weird url format, e.g., http://www.bing.com/cr?IG=1C80F8C1C1B04D4C866FD62099EF9E4E&CID=2291C45E2291669401DFCEFC23976733&rd=1&h=e9ZvWOedIV321QOg-FnBNtNHTR9Oo3Yqss9bCRYsT9o&v=1&r=http://www.cse.unsw.edu.au/%7Ecs9417ml/RL1/introduction.html&p=DevEx,5168.1. The following code remove the boilerplate
                url = url.split(",")[0].split("v=1&r=")[1];
                url = java.net.URLDecoder.decode(url, "UTF-8");
            }
            catch (Exception ex) {
                url = (String)item.get("displayUrl");
            }
			results.add(url);
			//System.out.println(url);
			
			JSONObject url_info = new JSONObject();
			url_info.put("link",url);
			url_info.put("rank",Integer.toString(nStart+i));
			download.addTask(url_info);
		    }
		}
	    }
	} 
	catch (MalformedURLException e1) {
	    e1.printStackTrace();
	} 
	catch (IOException e) {
	    e.printStackTrace();
	}
	catch (Exception e){
	    e.printStackTrace();
	}

	download.shutdown();
	System.out.println("Number of results: " + String.valueOf(results.size()));
	return results;
    }

    public static void main(String[] args) {
	
	String query = ""; //default
	String top = "50"; //default
    String start = "1"; //default

	String es_index = "memex";
	String es_doc_type = "page";
	String es_server = "localhost";
	
	int i = 0;
	while (i < args.length){
	    String arg = args[i];
	    if(arg.equals("-q")){
		query = args[++i];
	    } else if(arg.equals("-t")){ 
		top = args[++i];
	    } else if(arg.equals("-b")){
		start = args[++i];
	    } else if(arg.equals("-i")){
		es_index = args[++i];
	    } else if(arg.equals("-d")){
		es_doc_type = args[++i];
	    } else if(arg.equals("-s")){
		es_server = args[++i];
	    }else {
		System.err.println("Unrecognized option");
		break;
	    }
	    ++i;
	}

	BingSearch bs = new BingSearch();
	bs.search(query, start, top, es_index, es_doc_type, es_server);
    }
}
