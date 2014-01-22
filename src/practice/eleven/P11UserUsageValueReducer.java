package practice.eleven;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class P11UserUsageValueReducer extends Reducer<Text, Text, Text, Text>{
	
	private List<Text> userList = new ArrayList<Text>();
	private List<Text> usageList = new ArrayList<Text>();
	private String joinType ;
	
	@Override
	public void setup(Context context){
		joinType = context.getConfiguration().get("join.type");
	}
	
	
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		
		for(Text value : values){
			if(value.toString().startsWith("User_")){
				
				userList.add(new Text(value.toString().substring(5)));
			}else if(value.toString().startsWith("Usage_")){
				
				usageList.add(new Text(value.toString().substring(6)));
			}
		}
		
		if(joinType.equals("inner")){
			if(!userList.isEmpty() && !usageList.isEmpty()){
				for(Text user : userList){
					for(Text usage : usageList){
						context.write(user,  usage);
					}
				}
			}	
			
		}else if(joinType.equals("left-outer")){
			if(!userList.isEmpty()){
				for(Text user : userList){
					if(!usageList.isEmpty()){
						for(Text usage:usageList){
							context.write(user, usage);
						}
					}else{
						context.write(user, new Text(""));
					}
				}	
			}
			
		}else if(joinType.equals("right-outer")){
			if(!usageList.isEmpty()){
				for(Text usage : usageList){
					if(!userList.isEmpty()){
						for(Text user:userList){
							context.write(user, usage);
						}
					}else{
						context.write(new Text(""), usage);
					}
				}	
			}
			
		}else if(joinType.equals("full-outer")){
			
			if(!userList.isEmpty() && !usageList.isEmpty()){
				
				for(Text user : userList){
					for(Text usage : usageList){
						context.write(user, usage);
					}
				}
			}
			
			if(!userList.isEmpty()){
				if(usageList.isEmpty()){
					
					for(Text user : userList){
						context.write(user, new Text(""));
					}
				}
			}
			
			if(!usageList.isEmpty()){
				if(userList.isEmpty()){
					
					for(Text usage : usageList){
						context.write(new Text(""), usage);
					}
				}
			}
			
		}else if(joinType.equals("anti-join")){
			
			if( !userList.isEmpty() ^ !usageList.isEmpty()){
				if(!userList.isEmpty()){
					
					for(Text user : userList){
						context.write(user, new Text(""));
					}
					
				}else if(!usageList.isEmpty()){
					
					for(Text usage : usageList){
						context.write(new Text(""), usage);
					}
				}
			}
		}
		
		userList.clear();
		usageList.clear();
	}
	
	
}
