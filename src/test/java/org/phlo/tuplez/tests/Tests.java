package org.phlo.tuplez.tests;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.*;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import org.phlo.tuplez.*;
import org.phlo.tuplez.operation.*;
import org.phlo.tuplez.tests.Statements.Kind;

public class Tests {
	private static Executor executor;
	
	@BeforeClass
	public static void setUp() throws Throwable {
		/* Open in-memory database and create executor */
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		executor = new Executor(new SingleConnectionDataSource("jdbc:derby:memory:test;create=true", false));
		
		/* Set input for default.* */
		executor.setDefaultInput(new Statements());
		
		/* Create Schema */
		executor.with(Statements.CreateTest.class).execute();
		executor.with(Statements.CreateSingle.class).execute();
		Integer id = executor.with(Statements.InsertSingle.class).key();
		Assert.assertEquals(1, (int)id);
	}

	final static Calendar Calendar1 = Calendar.getInstance(Locale.ENGLISH);
	static {
		Calendar1.clear();
		Calendar1.setTimeZone(TimeZone.getDefault());
		Calendar1.set(2010, 10, 11, 0, 0, 0);
	}
	final static Calendar Calendar2 = Calendar.getInstance(Locale.ENGLISH);
	static {
		Calendar2.clear();
		Calendar2.setTimeZone(TimeZone.getDefault());
		Calendar2.set(1970, 0, 1, 0, 0, 0);
	}

	final static int ValuesCount = 2;
	final static String[] ValuesStr = new String[] {"foo", "bar"};
	final static BigDecimal[] ValuesDez = new BigDecimal[] {new BigDecimal("3.1415"), new BigDecimal("2.7")};
	final static Date[] ValuesDay = new Date[] {Calendar1.getTime(), Calendar2.getTime()};
	final static int[] ValuesIdx = new int[] { 42, 23};
	final static Kind[] ValuesKind = new Kind[] { Kind.KIND_OF_COOL, Kind.KIND_OF_SUCKS };

	public Statements.TestFull getDummyDataTestFull(final int id) {
		if ((id > 0) && (id <= ValuesCount)) {
			return new Statements.TestFull() {
				@Override public Long getId() { return (long)id; }
				@Override public String getStr() { return ValuesStr[id-1]; }
				@Override public BigDecimal getDez() { return ValuesDez[id-1]; }
				@Override public java.sql.Date getDay() { return new java.sql.Date(ValuesDay[id-1].getTime()); }
				@Override public Integer getIdx() { return ValuesIdx[id-1]; }
				@Override public Class<?> getNoSuchColumn() { return null; }
				@Override public String getDescription() { return null; }
				@Override public Kind getKind() { return ValuesKind[id-1]; }
			};
		}
		else {
			return new Statements.TestFull() {
				@Override public Long getId() { return (long)id; }
				@Override public String getStr() { return null; }
				@Override public BigDecimal getDez() { return null; }
				@Override public java.sql.Date getDay() { return null; }
				@Override public Integer getIdx() { return null; }
				@Override public Class<?> getNoSuchColumn() { return null; }
				@Override public String getDescription() { return null; }
				@Override public Kind getKind() { return null; }
			};
		}
	}
	
	@Before
	public void setupData() {
		executor.with(Statements.TestDelete.class).execute();

		for(int i = 0; i < ValuesCount; ++i)
			executor.with(Statements.TestInsert.class).execute(getDummyDataTestFull(i+1));
		
		executor.with(Statements.TestInsertId.class).execute((long)(ValuesCount + 1));
	}

	public void testFromFull(final Class<? extends Operation<Statements.TestFull, Statements.TestFull>> opClass) {
		for(int i = 0; i <= ValuesCount; ++i) {
			/* Get input */
			final Statements.TestFull testFull = getDummyDataTestFull(i+1);
			
			/* Run through DB */
			Statements.TestFull testFullOut = executor.with(Statements.TestFromFull.class).get(testFull);
			Assert.assertNotNull("One row expected, none found (id=" + (i+1) + ")", testFullOut);
			Assert.assertEquals(testFullOut, testFullOut);
			Assert.assertEquals(testFullOut.hashCode(), testFullOut.hashCode());
			
			/* Compare fields */
			Assert.assertEquals(testFull.getId(), testFullOut.getId());
			Assert.assertEquals(testFull.getStr(), testFullOut.getStr());
			if ((testFull.getDez() != null) && (testFullOut.getDez() != null))
				Assert.assertEquals(testFull.getDez().stripTrailingZeros(), testFullOut.getDez().stripTrailingZeros());
			else
				Assert.assertEquals(testFull.getDez(), testFullOut.getDez());
			Assert.assertEquals(testFull.getDay(), testFullOut.getDay());
			Assert.assertEquals(testFull.getIdx(), testFullOut.getIdx());
			Assert.assertEquals((new Statements()).getDescription(), testFullOut.getDescription());
			Assert.assertEquals(testFull.getNoSuchColumn(), testFullOut.getNoSuchColumn());
			
			/* Validate toString() */
			Assert.assertEquals(
				"{" +
					"getId(): " + testFullOut.getId() + "; " +
					"getStr(): " + testFullOut.getStr() + "; " +
					"getDez(): " + testFullOut.getDez() + "; " +
					"getDay(): " + testFullOut.getDay() + "; " +
					"getIdx(): " + testFullOut.getIdx() + "; " +
					"getDescription(): " + (new Statements()).getDescription() + "; " +
					"getKind(): " + testFullOut.getKind() +
				"}",
				testFullOut.toString()
			);
		}
	}
	
	
	@Test
	public void testFromFullNotAnnotated() {
		testFromFull(Statements.TestFromFull.class);
	}

	@Test
	public void testFromFullAnnotated() {
		testFromFull(Statements.TestFromFullAnnotated.class);
	}
	
	@Test
	public void testIdToFull() {
		for(int i = 0; i <= ValuesCount; ++i) {
			/* Run through DB */
			Statements.TestFull testFullOut = executor.with(Statements.TestIdToFull.class).get(i+1);
			Assert.assertNotNull("One row expected, none found (id=" + (i+1) + ")", testFullOut);
			
			/* Expected Output */
			final Statements.TestFull testFullExpected = getDummyDataTestFull(i+1);
			
			/* Compare fields */
			Assert.assertEquals(testFullExpected.getId(), testFullOut.getId());
			Assert.assertEquals(testFullExpected.getStr(), testFullOut.getStr());
			if ((testFullExpected.getDez() != null) && (testFullOut.getDez() != null))
				Assert.assertEquals(testFullExpected.getDez().stripTrailingZeros(), testFullOut.getDez().stripTrailingZeros());
			else
				Assert.assertEquals(testFullExpected.getDez(), testFullOut.getDez());
			Assert.assertEquals(testFullExpected.getDay(), testFullOut.getDay());
			Assert.assertEquals(testFullExpected.getIdx(), testFullOut.getIdx());
			Assert.assertEquals((new Statements()).getDescription(), testFullOut.getDescription());
			Assert.assertEquals(testFullExpected.getNoSuchColumn(), testFullOut.getNoSuchColumn());
		}
	}
	
	@Test
	public void testGeneratedKey() {
		Long id = executor.with(Statements.TestInsertGenerateId.class).key(new Statements.TestNew() {
			@Override public String getStr() { return null; }
			@Override public BigDecimal getDez() { return null; }
			@Override public java.sql.Date getDay() { return null; }
			@Override public Integer getIdx() { return null; }
			@Override public Class<?> getNoSuchColumn() { return null; }
			@Override public String getDescription() { return null; }
			@Override public Kind getKind() { return null; }
		});
		
		Assert.assertEquals(1000L, (long)id);
	}
	
	@Test
	public void testIdToStr() {
		Assert.assertEquals("foo", executor.with(Statements.TestIdToStr.class).get(1L));
		Assert.assertEquals("bar", executor.with(Statements.TestIdToStr.class).get(2L));
		Assert.assertEquals(null, executor.with(Statements.TestIdToStr.class).get(3L));
	}
	
	@Test
	public void testAllFull() {
		long expectedId = 1;
		for(Statements.TestFull testFull: executor.with(Statements.TestAllFull.class).collection()) {
			Assert.assertEquals((long)expectedId, (long)testFull.getId());
			
			++expectedId;
		}
	}
	
	@Test(expected=java.util.NoSuchElementException.class)
	public void testIterationExceed() {
		executor.with(Statements.TestResultSize.class).iterate(1, new IteratorProcessor<String, Void>() {
			@Override
			public Void processIterator(Iterator<String> iterator) {
				String row = iterator.next();
				Assert.assertEquals("Row 1", row);
				iterator.next();
				return null;
			}
		});
	}

	@Test(expected=org.springframework.dao.IncorrectResultSizeDataAccessException.class)
	public void testWrongSingleResultAnnotation() {
		executor.with(Statements.TestResultSize.class).get(2);
	}
	
	@Test
	public void testNonStaticOperation() {
		try {
			executor.with(Statements.TestNonStatic.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNonStatic.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("operation is a non-static class")
			);
		}
	}
	
	@Test
	public void testNoDefaultConstructor() {
		try {
			executor.with(Statements.TestNoDefaultConstructor.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNoDefaultConstructor.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("unable to instantiate operation")
			);
		}
	}

	@Test
	public void testNoStatement() {
		try {
			executor.with(Statements.TestNoStatement.class).execute();
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNoStatement.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class neither implements OperationStatementIsComputed nor does it carry @Statement annotation")
			);
		}
	}
	
	@Test
	public void testAmbiguousStatement() {
		try {
			executor.with(Statements.TestAmbiguousStatement.class).execute();
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestAmbiguousStatement.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class both implements OperationStatementIsComputed and carries a @Statement annotation")
			);
		}
	}	
}