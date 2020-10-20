package tr.com.beb.SimpleTextParser.repository;

import tr.com.beb.SimpleTextParser.exception.RepositoryException;

public interface DataRepository<T> {

	T read() throws RepositoryException;
	
	void write(T resource, String outputName) throws RepositoryException;
	
	
}
