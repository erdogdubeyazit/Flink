package tr.com.beb.SimpleTextParser.util;

import tr.com.beb.SimpleTextParser.exception.ApplicationException;

public class ArgumentHandler {
	
	private String source = null;
	private String target = null;
	
	public void handle(String[] args) throws ApplicationException {
		
		if(args.length!=2)
			throw new ApplicationException("Insufficient arguments");
		else {
			for(String arg : args) {
				String[] split = arg.split("=");
				if (split.length!=2)
					throw new ApplicationException("Malformatted arguments");
				if(split[0].equalsIgnoreCase("input") || split[0].equalsIgnoreCase("--input"))
					this.source=split[1];
				if(split[0].equalsIgnoreCase("output") || split[0].equalsIgnoreCase("--output"))
					this.target=split[1];
			}
			
			if(source == null)
				source = args[0];
			if(target == null)
				target = args[1];
			
		}
		
	}

	public String getSource() {
		return source;
	}

	public String getTarget() {
		return target;
	}

	

}
