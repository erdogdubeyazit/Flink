package tr.com.beb.SimpleTextParser;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tr.com.beb.SimpleTextParser.repository.DataRepository;
import tr.com.beb.SimpleTextParser.repository.impl.FlinkFileRepository;
import tr.com.beb.SimpleTextParser.service.ParserService;
import tr.com.beb.SimpleTextParser.service.impl.FlinkParserService;

@SuppressWarnings("rawtypes")
@ExtendWith(MockitoExtension.class)
public class FlinkParsingTest {

	@Mock
	private DataRepository dataRepository;

	@Mock
	private ParserService parserService;

	@Mock
	private ExecutionEnvironment env;

	@SuppressWarnings({ "static-access", "unchecked" })
	@BeforeEach
	void setUp() {

		dataRepository = Mockito.mock(FlinkFileRepository.class);
		env = env.getExecutionEnvironment();
		try {
			Mockito.when(dataRepository.read()).thenReturn(
					env.fromElements("date|productId|eventName|userId", "1111|477|view|47", "1111|477|add|47",
							"1111|477|remove|47", "1111|477|click|47", "1111|477|view|48", "1111|477|add|48",
							"1111|477|remove|48", "1111|477|click|48", "1111|477|view|49", "1111|477|add|49",
							"1111|477|remove|49", "1111|477|click|49", "1111|477|view|50", "1111|477|add|50",
							"1111|477|remove|50", "1111|477|click|50", "1111|477|view|51", "1111|477|add|51",
							"1111|477|remove|51", "1111|477|click|51", "1111|477|view|52", "1111|477|add|52"));

			parserService = new FlinkParserService(dataRepository);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	void testViewCountsByProductId() {

		try {
			DataSet viewCountsByProductId = (DataSet) parserService.getViewCountsByProductId();

			List elements = viewCountsByProductId.collect();

			assertThat(((Tuple2) elements.get(0)).f0).isEqualTo("477");
			assertThat(((Tuple2) elements.get(0)).f1).isEqualTo(6);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	void testEventCounts() {

		try {

			DataSet eventCounts = (DataSet) parserService.getEventCounts();

			List elements = eventCounts.collect();
			
			assertThat(((Tuple2) elements.get(0)).f0).isEqualTo("add");
			assertThat(((Tuple2) elements.get(0)).f1).isEqualTo(6);

			assertThat(((Tuple2) elements.get(1)).f0).isEqualTo("click");
			assertThat(((Tuple2) elements.get(1)).f1).isEqualTo(5);
			
			assertThat(((Tuple2) elements.get(2)).f0).isEqualTo("remove");
			assertThat(((Tuple2) elements.get(2)).f1).isEqualTo(5);


			assertThat(((Tuple2) elements.get(3)).f0).isEqualTo("view");
			assertThat(((Tuple2) elements.get(3)).f1).isEqualTo(6);


		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	@SuppressWarnings("unchecked")
	void testEventCounstInvokedByUser() {

		try {

			DataSet<Tuple2<String, Integer>> eventCounstInvokedByUser = ((DataSet<Tuple2<String, Integer>>) parserService.getEventCounstInvokedByUser(47));

			List<Tuple2<String, Integer>> elements = eventCounstInvokedByUser.collect();
			
			List<Tuple2<String, Integer>> expected = new ArrayList<Tuple2<String, Integer>>();
			
			expected.add(new Tuple2<String, Integer>("add",1));
			expected.add(new Tuple2<String, Integer>("click",1));
			expected.add(new Tuple2<String, Integer>("remove",1));
			expected.add(new Tuple2<String, Integer>("view",1));
			
			assertThat(elements).containsExactlyElementsOf(expected);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	void testProductViewsByUserId() {

		try {

			DataSet productViewsByUserId = (DataSet) parserService.getProductViewsByUserId(47);

			List elements = productViewsByUserId.collect();

			assertThat(elements.get(0)).isEqualTo(477);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void testGetTopUsersFulfilledAllTheEvents() {

		try {

			DataSet topUsersFulfilledAllTheEvents = (DataSet) parserService.getTopUsersFulfilledAllTheEvents(5);

			List<Integer> elements = topUsersFulfilledAllTheEvents.collect();

			assertThat(elements).containsAll(Arrays.asList(47, 48, 49, 50, 51));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
