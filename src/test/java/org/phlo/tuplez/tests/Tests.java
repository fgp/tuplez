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
		executor.execute(Statements.CreateTest.class);
		executor.execute(Statements.CreateSingle.class);
		Integer id = executor.key(Statements.InsertSingle.class);
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

	@Before
	public void setupData() {
		executor.execute(Statements.TestDelete.class);

		for(int i = 0; i < ValuesCount; ++i) {
			final int valIdx = i;

			executor.execute(Statements.TestInsert.class, new Statements.TestFull() {
				@Override public Long getId() { return (long)(valIdx + 1); }
				@Override public String getStr() { return ValuesStr[valIdx]; }
				@Override public BigDecimal getDez() { return ValuesDez[valIdx]; }
				@Override public java.sql.Date getDay() { return new java.sql.Date(ValuesDay[valIdx].getTime()); }
				@Override public Integer getIdx() { return ValuesIdx[valIdx]; }
				@Override public Class<?> getNoSuchColumn() { return null; }
				@Override public String getDescription() { return null; }
				@Override public Kind getKind() { return ValuesKind[valIdx]; }
			});
		}
		
		executor.execute(Statements.TestInsertId.class, (long)(ValuesCount + 1));
	}
	
	public void testFromFull(final Class<? extends Operation<Statements.TestFull, Statements.TestFull>> opClass) {
		for(int i = 0; i <= ValuesCount; ++i) {
			final int valIdx = i;
			final Statements.TestFull testFull;
			
			if (valIdx < ValuesCount) {
				testFull = new Statements.TestFull() {
					@Override public Long getId() { return (long)(valIdx + 1); }
					@Override public String getStr() { return ValuesStr[valIdx]; }
					@Override public BigDecimal getDez() { return ValuesDez[valIdx]; }
					@Override public java.sql.Date getDay() { return new java.sql.Date(ValuesDay[valIdx].getTime()); }
					@Override public Integer getIdx() { return ValuesIdx[valIdx]; }
					@Override public Class<?> getNoSuchColumn() { return null; }
					@Override public String getDescription() { return null; }
					@Override public Kind getKind() { return ValuesKind[valIdx]; }
				};
			}
			else {
				testFull = new Statements.TestFull() {
					@Override public Long getId() { return (long)(valIdx + 1); }
					@Override public String getStr() { return null; }
					@Override public BigDecimal getDez() { return null; }
					@Override public java.sql.Date getDay() { return null; }
					@Override public Integer getIdx() { return null; }
					@Override public Class<?> getNoSuchColumn() { return null; }
					@Override public String getDescription() { return null; }
					@Override public Kind getKind() { return null; }
				};
			}
			
			/* Run through DB */
			Statements.TestFull testFullOut = executor.get(Statements.TestFromFull.class, testFull);
			Assert.assertNotNull("One row expected, none found (valIdx=" + valIdx + ")", testFullOut);
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
	public void testIdToFull() {
		for(int i = 0; i <= ValuesCount; ++i) {
			final int valIdx = i;
			
			/* Run through DB */
			Statements.TestFull testFullOut = executor.get(Statements.TestIdToFull.class, (valIdx+1));
			Assert.assertNotNull("One row expected, none found (valIdx=" + valIdx + ")", testFullOut);
			Assert.assertEquals(testFullOut, testFullOut);
			Assert.assertEquals(testFullOut.hashCode(), testFullOut.hashCode());

			/* Expected Output */
			final Statements.TestFull testFullExpected;
			if (valIdx < ValuesCount) {
				testFullExpected = new Statements.TestFull() {
					@Override public Long getId() { return (long)(valIdx + 1); }
					@Override public String getStr() { return ValuesStr[valIdx]; }
					@Override public BigDecimal getDez() { return ValuesDez[valIdx]; }
					@Override public java.sql.Date getDay() { return new java.sql.Date(ValuesDay[valIdx].getTime()); }
					@Override public Integer getIdx() { return ValuesIdx[valIdx]; }
					@Override public Class<?> getNoSuchColumn() { return null; }
					@Override public String getDescription() { return null; }
					@Override public Kind getKind() { return ValuesKind[valIdx]; }
				};
			}
			else {
				testFullExpected = new Statements.TestFull() {
					@Override public Long getId() { return (long)(valIdx + 1); }
					@Override public String getStr() { return null; }
					@Override public BigDecimal getDez() { return null; }
					@Override public java.sql.Date getDay() { return null; }
					@Override public Integer getIdx() { return null; }
					@Override public Class<?> getNoSuchColumn() { return null; }
					@Override public String getDescription() { return null; }
					@Override public Kind getKind() { return null; }
				};
			}
			
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
	public void testGeneratedKey() {
		Long id = executor.key(Statements.TestInsertGenerateId.class, new Statements.TestNew() {
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
		Assert.assertEquals("foo", executor.get(Statements.TestIdToStr.class, 1L));
		Assert.assertEquals("bar", executor.get(Statements.TestIdToStr.class, 2L));
		Assert.assertEquals(null, executor.get(Statements.TestIdToStr.class, 3L));
	}
	
	@Test(expected=java.util.NoSuchElementException.class)
	public void testIterationExceed() {
		executor.iterate(Statements.TestResultSize.class, 1, new IteratorProcessor<String, Void>() {
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
		executor.get(Statements.TestResultSize.class, 2);
	}
	
	@Test
	public void testNonStaticOperation() {
		try {
			executor.execute(Statements.TestNonStatic.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNonStatic.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class is a non-static inner class")
			);
		}
	}
	
	@Test
	public void testNoDefaultConstructor() {
		try {
			executor.execute(Statements.TestNoDefaultConstructor.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNoDefaultConstructor.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class has no default constructor")
			);
		}
	}

	@Test
	public void testNoStatement() {
		try {
			executor.execute(Statements.TestNoStatement.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestNoStatement.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class neither implements StatementIsComputed nor does it carry @Statement annotation")
			);
		}
	}
	
	@Test
	public void testAmbiguousStatement() {
		try {
			executor.execute(Statements.TestAmbiguousStatement.class);
			Assert.fail("Expected exception but none thrown");
		}
		catch (InvalidOperationDefinitionException e) {
			Assert.assertEquals(Statements.TestAmbiguousStatement.class, e.getOperation());
			Assert.assertTrue("Wrong exception message: " + e.getMessage(),
				e.getMessage().contains("class both implements StatementIsComputed and carries a @Statement annotation")
			);
		}
	}	
}