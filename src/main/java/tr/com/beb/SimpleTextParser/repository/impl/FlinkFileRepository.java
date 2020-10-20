package tr.com.beb.SimpleTextParser.repository.impl;

import java.io.FileWriter;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tr.com.beb.SimpleTextParser.exception.RepositoryException;
import tr.com.beb.SimpleTextParser.repository.DataRepository;

@SuppressWarnings("rawtypes")
public class FlinkFileRepository implements DataRepository<DataSet> {

	private static final Logger logger = LoggerFactory.getLogger(FlinkFileRepository.class);

	private String source;
	private String targetDirectory;

	final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	public FlinkFileRepository(String source, String targetDirectory) {
		this.source = source;
		this.targetDirectory = targetDirectory;
	}

	@Override
	public DataSet read() throws RepositoryException {
		try {

			return env.readTextFile(source);
		} catch (Exception e) {
			logger.error("Error while reading file : " + source, e);
			throw new RepositoryException("Source file can not be read");
		}
	}

	@Override
	public void write(DataSet dataset, String outputName) throws RepositoryException {
		try {
			String path = targetDirectory + Path.SEPARATOR + outputName;
			if (dataset.getType().isTupleType())
				dataset.writeAsCsv(path, "\n", "|", WriteMode.OVERWRITE).setParallelism(1);
			else {
				FileWriter writer = new FileWriter(path);
				for (Object str : dataset.collect()) {
					writer.write(str + System.lineSeparator());
				}
				writer.close();

			}
		} catch (Exception e) {
			logger.error("Error while writing file : " + source, e);
			throw new RepositoryException("Source file can not be written");
		}

	}

}
