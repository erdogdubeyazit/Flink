package tr.com.beb.SimpleTextParser.service.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tr.com.beb.SimpleTextParser.exception.ServiceException;
import tr.com.beb.SimpleTextParser.repository.DataRepository;
import tr.com.beb.SimpleTextParser.service.ParserService;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class FlinkParserService implements ParserService<DataSet> {

	private static final Logger logger = LoggerFactory.getLogger(FlinkParserService.class);

	@SuppressWarnings("unused")
	private DataRepository<DataSet> dataRepository;
	private DataSet dataSet;

	public FlinkParserService(DataRepository<DataSet> dataRepository) throws ServiceException {
		this.dataRepository = dataRepository;
		try {
			dataSet = dataRepository.read().filter(new IncorrectLinesFilter());
		} catch (Exception e) {
			logger.error("Error while initializing FlinkParserService", e);
			throw new ServiceException("FlinkParserService initialization failed");
		}
	}

	@SuppressWarnings("serial")
	@Override
	public DataSet getViewCountsByProductId() throws ServiceException {
		try {
			DataSet<Tuple2<String, Integer>> result = dataSet
					.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

						@Override
						public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
							String[] tokens = value.split("\\|");

							if (tokens[2].equalsIgnoreCase("view")) {
								out.collect(new Tuple2<>(tokens[1], 1));
							}
						}
					}).groupBy(0).aggregate(Aggregations.SUM, 1);

			return result;
		} catch (Exception e) {
			logger.error("Error on getViewCountsByProductId operation", e);
			throw new ServiceException("getViewCountsByProductId operation failed");
		}
	}

	@SuppressWarnings("serial")
	@Override
	public DataSet getEventCounts() throws ServiceException {
		try {
			DataSet<Tuple2<String, Integer>> result = dataSet
					.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

						@Override
						public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
							String[] tokens = value.split("\\|");

							out.collect(new Tuple2<>(tokens[2], 1));

						}
					}).groupBy(0).aggregate(Aggregations.SUM, 1).sortPartition(0, Order.ASCENDING).setParallelism(1);

			return result;
		} catch (Exception e) {
			logger.error("Error on getEventCounts operation", e);
			throw new ServiceException("getEventCounts operation failed");
		}
	}

	@SuppressWarnings("serial")
	@Override
	public DataSet getTopUsersFulfilledAllTheEvents(final int topUsersLimit) throws ServiceException {
		try {
			DataSet result = dataSet.flatMap(new FlatMapFunction<String, Tuple3<Integer, String, Integer>>() {

				@Override
				public void flatMap(String value, Collector<Tuple3<Integer, String, Integer>> out) throws Exception {
					String[] tokens = value.split("\\|");
					out.collect(new Tuple3<Integer, String, Integer>(Integer.valueOf(tokens[3]), tokens[2], 1));
				}
			}).groupBy(0)
					.reduceGroup(new GroupReduceFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, Integer>>() {

						@Override
						public void reduce(Iterable<Tuple3<Integer, String, Integer>> values,
								Collector<Tuple2<Integer, Integer>> out) throws Exception {

							Set<String> operationNames = new HashSet<String>();
							Integer key = null;
							int sum = 0;
							for (Tuple3<Integer, String, Integer> t : values) {
								key = t.f0;
								operationNames.add(t.f1);
								sum += t.f2;
							}

							if (operationNames.size() == 4) {
								out.collect(new Tuple2<Integer, Integer>(key, sum));
							}

						}

					}).sortPartition(1, Order.DESCENDING).setParallelism(1).sortPartition(0, Order.ASCENDING).setParallelism(1).first(topUsersLimit)
					.map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {

						@Override
						public Integer map(Tuple2<Integer, Integer> value) throws Exception {
							return value.f0;
						}
					});

			return result;
		} catch (Exception e) {
			logger.error("Error on getTopUsersFulfilledAllTheEvents operation", e);
			throw new ServiceException("getTopUsersFulfilledAllTheEvents operation failed");
		}

	}

	class Result {
		Integer sayi;

	}

	@SuppressWarnings("serial")
	@Override
	public DataSet getEventCounstInvokedByUser(final int userId) throws ServiceException {

		try {
			DataSet<Tuple2<String, Integer>> result = dataSet
					.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

						@Override
						public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
							String[] tokens = value.split("\\|");

							if (Integer.valueOf(tokens[3]).intValue() == userId)
								out.collect(new Tuple2<>(tokens[2], 1));

						}
					}).groupBy(0).aggregate(Aggregations.SUM, 1).sortPartition(0, Order.ASCENDING).setParallelism(1);

			return result;
		} catch (Exception e) {
			logger.error("Error on getEventCounstInvokedByUser operation", e);
			throw new ServiceException("getEventCounstInvokedByUser operation failed");
		}
	}

	@SuppressWarnings("serial")
	@Override
	public DataSet getProductViewsByUserId(final int userId) throws ServiceException {
		try {
			DataSet result = dataSet.flatMap(new FlatMapFunction<String, Integer>() {

				@Override
				public void flatMap(String value, Collector<Integer> out) throws Exception {
					String[] tokens = value.split("\\|");
					if (Integer.valueOf(tokens[3]).intValue() == userId && tokens[2].equalsIgnoreCase("view"))
						out.collect(Integer.valueOf(tokens[1]));
				}
			}).distinct();

			return result;
		} catch (Exception e) {
			logger.error("Error on getProductViewsByUserId operation", e);
			throw new ServiceException("getProductViewsByUserId operation failed");
		}

	}

	@Override
	public void save(DataSet dataSet, String outputName) throws ServiceException {
		try {
			dataRepository.write(dataSet, outputName);
		} catch (Exception e) {
			logger.error("Error on save operation", e);
			throw new ServiceException("Error while saving the processed data");
		}

	}
}

@SuppressWarnings("serial")
class IncorrectLinesFilter implements FilterFunction<String> {

	@Override
	public boolean filter(String value) throws Exception {
		return (!value.equalsIgnoreCase("date|productId|eventName|userId") && value.split("\\|").length == 4);
	}

}
