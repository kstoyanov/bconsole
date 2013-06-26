package bg.viacont.beepster.console.resources;

import java.io.IOException;
import java.util.Properties;

public class BeepsterResources {

	static Properties properties = null;

	public final static Properties getProperties() throws IOException {
		if (null != properties)
			return properties;

		properties = new Properties();
		properties.load(BeepsterResources.class
				.getResourceAsStream("h2.properties"));
		return properties;
	}
}
