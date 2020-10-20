package tr.com.beb.SimpleTextParser;

import org.apache.flink.api.java.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tr.com.beb.SimpleTextParser.exception.ApplicationException;
import tr.com.beb.SimpleTextParser.exception.ServiceException;
import tr.com.beb.SimpleTextParser.repository.DataRepository;
import tr.com.beb.SimpleTextParser.repository.impl.FlinkFileRepository;
import tr.com.beb.SimpleTextParser.service.ParserService;
import tr.com.beb.SimpleTextParser.service.impl.FlinkParserService;
import tr.com.beb.SimpleTextParser.util.ArgumentHandler;

public class App {

	private static final Logger logger = LoggerFactory.getLogger(App.class);

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws ApplicationException {

		ArgumentHandler argumentHandler = new ArgumentHandler();
		argumentHandler.handle(args);

		DataRepository<DataSet> dataRepository = new FlinkFileRepository(argumentHandler.getSource(),
				argumentHandler.getTarget());

		ParserService<DataSet> parserService;
		try {
			parserService = new FlinkParserService(dataRepository);
		} catch (ServiceException e) {
			logger.error("Parser service initialization failed", e);
			throw new ApplicationException("Parser service initialization failed.");

		}

		try {
			DataSet viewCountsByProductId = parserService.getViewCountsByProductId();
			parserService.save(viewCountsByProductId, "ViewCountsByProductId.txt");
		} catch (Exception e) {
			logger.error("Error on viewCountsByProductId", e);
			throw new ApplicationException("Operation `viewCountsByProductId` failed.");
		}

		logger.info("viewCountsByProductId completed");

		try {
			DataSet eventCounts = parserService.getEventCounts();
			parserService.save(eventCounts, "EventCounts.txt");
		} catch (Exception e) {
			logger.error("Error on eventCounts", e);
			throw new ApplicationException("Operation `eventCounts` failed.");
		}

		logger.info("eventCounts completed");

		try {
			DataSet eventCounstInvokedByUser = parserService.getEventCounstInvokedByUser(47);
			parserService.save(eventCounstInvokedByUser, "EventCounstInvokedByUser.txt");
		} catch (Exception e) {
			logger.error("Error on eventCounstInvokedByUser", e);
			throw new ApplicationException("Operation `eventCounstInvokedByUser` failed.");
		}

		logger.info("eventCounstInvokedByUser completed");

		try {
			DataSet productViewsByUserId = parserService.getProductViewsByUserId(47);
			parserService.save(productViewsByUserId, "ProductViewsByUserId.txt");
		} catch (Exception e) {
			logger.error("Error on productViewsByUserId", e);
			throw new ApplicationException("Operation `productViewsByUserId` failed.");
		}

		logger.info("productViewsByUserId completed");

		try {
			DataSet topUsersFulfilledAllTheEvents = parserService.getTopUsersFulfilledAllTheEvents(5);
			parserService.save(topUsersFulfilledAllTheEvents, "TopUsersFulfilledAllTheEvents.txt");
		} catch (Exception e) {
			logger.error("Error on topUsersFulfilledAllTheEvents", e);
			throw new ApplicationException("Operation `topUsersFulfilledAllTheEvents` failed.");
		}

		logger.info("topUsersFulfilledAllTheEvents completed");

	}

}
