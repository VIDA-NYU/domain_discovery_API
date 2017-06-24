import org.json.JSONObject;
import java.util.ArrayList;
import java.util.Arrays;

public class Download_urls {
    public Download_urls(){
    }
    
    public void download(String[] urls, String query, String subquery, ArrayList<String>  tag, String es_index, String es_doc_type, String es_server){
	Download download = new Download(query, subquery, tag, es_index, es_doc_type, es_server);
	
	for(String url: urls){
	    JSONObject url_info = new JSONObject();
	    url_info.put("link",Download_Utils.validate_url(url));
	    download.addTask(url_info);
	}
	
	download.shutdown();
	//System.out.println("Number of results: " + urls.length);
			    
    }

    public static void main(String[] args) {
	
	String urls_str = ""; //default
	String es_index = "memex";
	String es_doc_type = "page";
	String es_server = "localhost";
	String query = "uploaded";
	String subquery = null;
	String tag = "";
	
	int i = 0;
	while (i < args.length){
	    String arg = args[i];
	    if(arg.equals("-u")){
		urls_str = args[++i];
	    } else if(arg.equals("-i")){
		es_index = args[++i];
	    } else if(arg.equals("-d")){
		es_doc_type = args[++i];
	    } else if(arg.equals("-s")){
		es_server = args[++i];
	    } else if(arg.equals("-q")){
		query = args[++i];
	    } else if(arg.equals("-t")){
		tag = args[++i];
	    } else if(arg.equals("-sq")){
		subquery = args[++i];
	    }else {
		System.err.println("Unrecognized option");
		break;
	    }
	    ++i;
	}

	String[] urls = null;
	if(urls_str != null & !urls_str.isEmpty())
	    urls = urls_str.split(" ");
	
	ArrayList<String> tags = new ArrayList<String>();
	String[] tmp_tags = tag.split(",");
	for(i=0;i < tmp_tags.length;++i){
	    tags.add(tmp_tags[i]);
	}
	
	Download_urls download_urls = new Download_urls();
	download_urls.download(urls, query, subquery, tags, es_index, es_doc_type, es_server);
    }
}
