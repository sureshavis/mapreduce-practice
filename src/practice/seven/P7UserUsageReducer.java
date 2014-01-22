package practice.seven;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class P7UserUsageReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
		String userStr = null;
		StringBuilder usageStr = new StringBuilder();
		
		for(Text value:values){
			
			if(value.toString().startsWith("e")){
				userStr = value.toString();
			}else{
				usageStr.append(value.toString()+",");
			}
			
		}
		
		try {
			context.write(new Text(constructXMLTree(userStr, usageStr.toString())),NullWritable.get());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}

	
	public String constructXMLTree(String userDetails,String usageDetails) throws ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException{
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		Document doc = docFactory.newDocumentBuilder().newDocument();	
		
		Element rootElement = doc.createElement("user-usage");
		
		doc.appendChild(rootElement);
		
		
		Element userElement = doc.createElement("user");
		rootElement.appendChild(userElement);
		
		String [] columnValues = userDetails.split("\t");
		
		Attr attr1 = doc.createAttribute("id");
		attr1.setNodeValue(columnValues[0]);
		userElement.setAttributeNode(attr1);
		
		String [] usageEntries = usageDetails.split(",");
		for(String usageStr : usageEntries){
			Element usageElement = doc.createElement("usage");
			usageElement.setTextContent(usageStr.split("\t")[0]);
			userElement.appendChild(usageElement);
		}
		
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		StringWriter stringWriter = new StringWriter();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		
		transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
		
		//System.out.println(stringWriter.getBuffer().toString());
		
		return stringWriter.getBuffer().toString();
	}
/*	
	public static void main(String[] args) throws ParserConfigurationException, TransformerFactoryConfigurationError, TransformerException {
		new P7UserUsageReducer().constructXMLTree("e001    suresh  24      chennai se      1       10", "u1      e001    1.1.1.1 1357648060      1357648070");
	}*/
}
