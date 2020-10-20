package tr.com.beb.SimpleTextParser.service;

import tr.com.beb.SimpleTextParser.exception.ServiceException;

public interface ParserService<T> {
	
	/**
	 * @return the counts of events labeled as "view" per product
	 * 
	 * @throws ServiceException
	 */
	T getViewCountsByProductId() throws ServiceException;
	
	/**
	 * 
	 * @return the counts by event
	 * @throws ServiceException
	 */
	T getEventCounts() throws ServiceException;

	/**
	 * 
	 * @param topUsersLimit
	 * @return the top n userIds who invoked all events
	 * @throws ServiceException
	 */
	T getTopUsersFulfilledAllTheEvents(int topUsersLimit) throws ServiceException;
	
	/**
	 * 
	 * @return the event count per userId
	 * 
	 * @param userId
	 * @throws ServiceException
	 */
	T getEventCounstInvokedByUser(int userId) throws ServiceException;

	/**
	 * 
	 * @param userId
	 * @return productIds `viewed` by userId
	 * @throws ServiceException
	 */
	T getProductViewsByUserId(int userId) throws ServiceException;
	
	/**
	 * Saves the processed data
	 * @param dataSet
	 * @param outputName
	 * @throws ServiceException
	 */
	void save(T dataSet, String outputName) throws ServiceException;
		
}
