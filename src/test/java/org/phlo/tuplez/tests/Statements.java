package org.phlo.tuplez.tests;

import java.math.BigDecimal;

import org.phlo.tuplez.operation.*;

public class Statements {
	public String getDescription() {
		return "test entry";
	}
	
	public interface TestNew {
		String getStr();
		BigDecimal getDez();
		java.sql.Date getDay();
		Integer getIdx();
		String getDescription();
		Class<?> getNoSuchColumn();
	}
	
	public interface TestFull extends TestNew {
		Long getId();
	}

	
	@Statement("CREATE TABLE test (" +
		"id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1000) NOT NULL PRIMARY KEY, " +
		"str VARCHAR(256) DEFAULT NULL, " +
		"dez DECIMAL(31,15) DEFAULT NULL, " +
		"day DATE DEFAULT NULL," +
		"idx INTEGER DEFAULT NULL," +
		"des VARCHAR(256)" +
	")")
	public interface CreateTest extends Operation<Void, Void> {}
	
	
	@Statement("CREATE TABLE SINGLE (unit INT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) NOT NULL)")
	public interface CreateSingle extends Operation<Void, Void> {}

	
	@Statement("INSERT INTO SINGLE (unit) VALUES (DEFAULT)")
	public interface InsertSingle extends GeneratesKey<Integer>, ReturnsSingleRow, Operation<Void, Void> {}

		
	@Statement("DELETE FROM test")
	public interface TestDelete extends Operation<Void, Void> {}
	
	
	@Statement("INSERT INTO test (" +
			"id, str, dez, day, idx, des" +
		") VALUES (" +
			":in.id, :in.str, :in.dez, :in.day, :in.idx, :default.description" +
	")")
	public interface TestInsert extends Operation<TestFull, Void> {}
	
	
	@Statement("INSERT INTO test (" +
			"str, dez, day, idx, des" +
		") VALUES (" +
			":in.str, :in.dez, :in.day, :in.idx, :default.description" +
	")")
	@KeyColumn("ID")
	public interface TestInsertGenerateId extends ReturnsSingleRow, GeneratesKey<Long>, Operation<TestNew, Void> {}

	
	@Statement("INSERT INTO test (" +
			"id, des "+
		") VALUES (" +
			":in, :default.description" +
	")")
	public interface TestInsertId extends Operation<Long, Void> {}

	
	@Statement("SELECT id, str, dez, day, idx, des as \"description\" " +
		"FROM test " +
		"WHERE " +
			"(id = :in.id OR (id IS NULL AND :in.id IS NULL)) AND " +
			"(str = :in.str OR (str IS NULL AND :in.str IS NULL)) AND " +
			"(dez = :in.dez OR (dez IS NULL AND :in.dez IS NULL)) AND " +
			"(day = :in.day OR (day IS NULL AND :in.day IS NULL)) AND " +
			"(idx = :in.idx OR (idx IS NULL AND :in.idx IS NULL))"
	)
	public interface TestFromFull extends ReturnsSingleRow, Operation<TestFull, TestFull> {}

	
	@Statement("SELECT " +
			"id, str as string, dez as dezimal, day AS \"day\", idx AS \"idx\", des " +
		"FROM test " +
		"WHERE " +
			"(id = :in.id OR (id IS NULL AND :in.id IS NULL)) AND " +
			"(str = :in.str OR (str IS NULL AND :in.str IS NULL)) AND " +
			"(dez = :in.dez OR (dez IS NULL AND :in.dez IS NULL)) AND " +
			"(day = :in.day OR (day IS NULL AND :in.day IS NULL)) AND " +
			"(idx = :in.idx OR (idx IS NULL AND :in.idx IS NULL))"
	)
	@OutputMapping({
		@OutputColumn(column="ID", getter="getId"),
		@OutputColumn(column="STRING", getter="getStr"),
		@OutputColumn(column="DECIMAL", getter="getDez", jdbcAccessor="getBigDecimal"),
		@OutputColumn(column="DAY", jdbcAccessor="getObject"),
		@OutputColumn(column="DES", getter="getDescription")
	})
	public interface TestFromFullAnnotated extends TestFromFull {}
	
	
	@Statement("SELECT str FROM test WHERE id = :in")
	public interface TestIdToStr extends ReturnsSingleRow, Operation<Long, String> {}
	
	
	@Statement("SELECT id, str, dez, day, idx, des as \"description\" " +
		"FROM test " +
		"WHERE id = :in"
	)
	public interface TestIdToFull extends ReturnsSingleRow, Operation<Integer, TestFull> {}
	
	
	public final static class TestResultSize implements
		Operation<Integer, String>, ReturnsSingleRow,
		StatementIsComputed<Integer>
	{
		@Override
		public String getStatement(Integer input) {
			StringBuilder b = new StringBuilder();
			for(int i=0; i < input; ++i) {
				if (i > 0)
					b.append(" union all ");
				b.append("select 'Row " + (i+1) + "' from single");
			}
			return b.toString();
		}
	}
	
	
	
	public final class TestNonStatic implements
		Operation<Void, Void>,
		StatementIsComputed<Void>
	{
		@Override public String getStatement(Void input) { return null; }
	}

	
	public final static class TestNoDefaultConstructor implements
		Operation<Void, Void>,
		StatementIsComputed<Void>
	{
		public TestNoDefaultConstructor(String dummy) {}
		@Override public String getStatement(Void input) { return null; }
	}

	
	public interface TestNoStatement extends Operation<Void, Void> {}
	
	
	@Statement("")
	public final class TestAmbiguousStatement implements
		Operation<Void, Void>,
		StatementIsComputed<Void>
	{
		@Override public String getStatement(Void input) { return null; }
	}
}
	
