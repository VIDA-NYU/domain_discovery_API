import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.ArrayList;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.net.URI;
import java.net.URL;
import java.net.MalformedURLException;
import org.json.JSONObject;
import org.json.JSONException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.Header;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.util.EntityUtils;
import org.apache.http.params.HttpParams;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
//import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.http.client.HttpClient;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.update.UpdateRequest;


import org.elasticsearch.action.index.IndexResponse;

public class Download_URL implements Runnable {
    String url = "";
    String description = "";
    String title = "";
    String query = "";
    String subquery = null;    
    String rank = "0";
    String es_index = "memex";
    String es_doc_type = "page";
    String es_host = "";
    Client client = null;
    Integer timeout = 5; 
	
    public Download_URL(JSONObject url_info, String query, String subquery, String es_index, String es_doc_type, Client client){
	try{
	    this.url = (String)url_info.get("link");
	}catch(JSONException e){
	    e.printStackTrace();
	}

	if (url_info.has("snippet"))
	    this.description = (String)url_info.get("snippet");
	if (url_info.has("title"))
	    this.title = (String)url_info.get("title");
	if (url_info.has("rank"))
	    this.rank = (String)url_info.get("rank");
	
	this.query = query;
	this.subquery = subquery;	
	this.client = client;
	if(!es_index.isEmpty())
	    this.es_index = es_index;
	if(!es_doc_type.isEmpty())
	    this.es_doc_type = es_doc_type;
    }

    public String getDescription(String responseBody, String content_text){
	// try to extract og:description or the first <meta name="description"> tag available in the html
	responseBody = responseBody.trim();

	String desc = "";
	Pattern p = Pattern.compile("<meta property=\"og:description\" content=\"(.*?)\"(.*?)/>", Pattern.CASE_INSENSITIVE);
	Matcher m = p.matcher(responseBody);

	if(m.find()) desc = m.group(1);
	else {
	    p = Pattern.compile("<meta content=\"(.*?)\" property=\"og:description\"(.*?)", Pattern.CASE_INSENSITIVE);
	    m = p.matcher(responseBody);

	    if(m.find()) {
		desc = m.group(1);
		desc = desc.substring(desc.lastIndexOf("\"")+1);
	    }
	    else {
		p = Pattern.compile("<meta name=\"description\"(.*?)content=\"(.*?)\"(.*?)>", Pattern.CASE_INSENSITIVE);
		m = p.matcher(responseBody);
		if(m.find()) desc = m.group(2);
	    }
	}

	String clean = "";
	if(!desc.equals("")) {
            clean = desc + " " + content_text;
	} else {
            clean = content_text;
	}

	clean = clean.replace("\\n"," ");
	clean = clean.replace("\\s\\s+", " " );

	return clean;
    }

    public String getImage(String responseBody, URL url){
	// try to extract og:image or the first <img> tag available in the html
	responseBody = responseBody.trim();

  String img_url = "";
	Pattern p = Pattern.compile("<meta .*?=\"og:image\" content=\"(.*?)\"(.*?)", Pattern.CASE_INSENSITIVE);
	Matcher m = p.matcher(responseBody);

	if(m.find()){
      img_url = m.group(1);
    }
	else {
	    p = Pattern.compile("<meta content=\"(.*?)\" .*?=\"og:image\"", Pattern.CASE_INSENSITIVE);
	    m = p.matcher(responseBody);

	    if(m.find()) img_url = m.group(1);
	    else {
		p = Pattern.compile("<img(.*?)src=\"(.*?)\"", Pattern.CASE_INSENSITIVE);
		m = p.matcher(responseBody.substring(responseBody.indexOf("<body"), responseBody.length()-1));
		if(m.find()){
		    img_url = m.group(2);
      }
		else return "";
	    }
	}

	// could find a image
	// try to fix or resolve relative URLs
	if(img_url.indexOf("http://") == 0 ||
	   img_url.indexOf("https://") == 0) { // complete URL found
	    return img_url;
	}
	if(img_url.indexOf("//") == 0) { // URL without protocol found
	    return "http:"+img_url;
	}
	//relative URL found
	try{
	    img_url = new URL(url, img_url).toString();
	}catch (MalformedURLException e){
	    System.err.println("MalformedURLException " + e.getMessage());
	}

	return img_url;
    }

    public void run() {
	long startTime = System.currentTimeMillis();
	long elapsedTime = 0L;

	//Do not process pdf files
	if(this.url.contains(".pdf"))
	    return;
	
	// Perform a GET request
	HttpUriRequest request = new HttpGet(url);

	// Set timeout for http request
	RequestConfig config = RequestConfig.custom()
	    .setConnectTimeout(timeout * 1000)
	    .setConnectionRequestTimeout(timeout * 1000)
	    .setSocketTimeout(timeout * 1000).build();
	
	CloseableHttpClient httpclient = 
	    HttpClientBuilder.create().setDefaultRequestConfig(config).build();
	
	URI url = request.getURI();
	String domain = url.getHost();

	HttpResponse response = null;
	try{
	    response = httpclient.execute(request);

	    int status = response.getStatusLine().getStatusCode();

	    if (status >= 200 && status < 300) {
		HttpEntity entity = response.getEntity();
		if(entity != null){

		    String responseBody = EntityUtils.toString(entity);

		    String content_type = response.getFirstHeader("Content-Type").getValue();
		    Integer content_length = (response.getFirstHeader("Content-Length") != null) ? Integer.valueOf(response.getFirstHeader("Content-Length").getValue()) : responseBody.length();
		    Map extracted_content = null;
		    if(content_type.contains("text/html")){
			Extract extract = new Extract();
			extracted_content = extract.process(responseBody);
		    }

		    if (extracted_content != null) {
			String content_text = (String)extracted_content.get("content");

			if(title.isEmpty())
			    title = (String)extracted_content.get("title");

			if(description.isEmpty())
			    description = getDescription(responseBody, content_text);

			String imageUrl = getImage(responseBody, url.toURL());

			SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			date_format.setTimeZone(TimeZone.getTimeZone("UTC"));
			String timestamp = date_format.format(new Date());

			SearchResponse searchResponse = null;
			searchResponse = client.prepareSearch(this.es_index)
			    .setTypes(this.es_doc_type)
			    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			    .setFetchSource(new String[]{"query", "subquery"}, null)
			    .setQuery(QueryBuilders.termQuery("url", url))
			    .setFrom(0).setExplain(true)
			    .execute()
			    .actionGet();

			// Construct the object to be updated for the url
			XContentBuilder jobj = XContentFactory.jsonBuilder().startObject();
			jobj.field("html", responseBody)
			    .field("text", content_text)
			    .field("title", title)
			    .field("length", content_length)
			    .field("retrieved", timestamp)
			    .field("image_url", new URI(imageUrl))
			    .field("description", description)
			    .field("rank", this.rank);

			SearchHit[] hits = searchResponse.getHits().getHits();
			for (SearchHit hit : searchResponse.getHits()) {
			    Map map = hit.getSource();
			    ArrayList query_list = (ArrayList)map.get("query");
			    ArrayList subquery_list = (ArrayList)map.get("subquery");
			    if(!query_list.contains(this.query)){
				query_list.add(this.query);
				jobj.field("query", query_list);
			    }
			
			    if(this.subquery != null && subquery_list != null && !subquery_list.contains(this.subquery)){
				jobj.field("subquery", subquery_list);
			    }

			    UpdateRequest updateRequest = new UpdateRequest(this.es_index, this.es_doc_type, hit.getId()).doc(jobj.endObject());
			    this.client.update(updateRequest).get();
			}

			if(hits.length == 0){
			    jobj.field("url", url);
			    jobj.field("domain", domain);
			    
			    jobj.field("query", new String[]{this.query});

			    if(this.subquery != null)
				jobj.field("subquery", new String[]{this.subquery});				     				       

			    IndexResponse indexresponse = this.client.prepareIndex(this.es_index, this.es_doc_type)
				.setSource(jobj.endObject())
				.execute()
				.actionGet();
			}
		    }
		} else {
		    httpclient.close();
		}
	    } else {
		httpclient.close();
		throw new ClientProtocolException("Unexpected response status: " + status);
	    }
	} catch (ClientProtocolException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	} catch (IOException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}
	finally {
	    try{
		httpclient.close();
	    } catch (IOException e){
		e.printStackTrace();
	    }
        }
	
	elapsedTime = (new Date()).getTime() - startTime;
	System.err.println("\n\n\nTime Elapsed time for " + url + " thread = "+String.valueOf(elapsedTime/1000.0)+" secs \n\n\n");
    }
}
