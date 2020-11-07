using System;
using Xunit;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Helpers;

namespace Rochas.DapperRepository.Test
{
    public class EntitySqlParserTest
    {
        [Fact]
        public void GetByPrimaryKeyTest()
        {
            var entityType = typeof(SampleEntity);
            var entityProps = entityType.GetProperties();
            var testFilter = EntityReflector.GetFilterByPrimaryKey(entityType, entityProps, 12345);
            
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Get, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void GetByFilterTest()
        {
            var testFilter = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.Get, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void ListTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.List, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} LIKE '%roberto%'", "sample_entity", "name"), result);
        }

        [Fact]
        public void ListLimitedTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.List, testFilter, 5);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.Contains(string.Format("WHERE {0}.{1} LIKE '%roberto%'", "sample_entity", "name"), result);
            Assert.EndsWith("LIMIT 5", result);
        }

        [Fact]
        public void ListLimitedSQLServerTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLServer, PersistenceAction.List, testFilter, 5);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("TOP 5", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} LIKE '%roberto%'", "sample_entity", "name"), result);
        }

        [Fact]
        public void SearchTest()
        {
            var filterType = typeof(SampleEntity);
            var filterProps = filterType.GetProperties();
            var testFilter = EntityReflector.GetFilterByFilterableColumns(typeof(SampleEntity), filterProps, "roberto");

            var result = EntitySqlParser.ParseEntity(testFilter, DatabaseEngine.SQLite, PersistenceAction.List, testFilter);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} LIKE '%roberto%'", "sample_entity", "name"), result);
        }

        [Fact]
        public void CreateTest()
        {
            var sampleEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(sampleEntity, DatabaseEngine.SQLite, PersistenceAction.Create);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("INSERT INTO", result);
            Assert.Contains("VALUES", result);
            Assert.Contains("creation_date", result);
            Assert.Contains("name", result);
            Assert.Contains("active", result);
        }

        [Fact]
        public void EditTest()
        {
            var editedEntity = new SampleEntity() { Name = "roberto gomes", Age = 35 };
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(editedEntity, DatabaseEngine.SQLite, PersistenceAction.Edit, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("UPDATE", result);
            Assert.Contains("SET", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void DeleteTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, DatabaseEngine.SQLite, PersistenceAction.Delete, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("DELETE FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }

        [Fact]
        public void CountTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, DatabaseEngine.SQLite, PersistenceAction.Count, filterEntity);
            result = result.Trim();

            Assert.NotNull(result);
            Assert.StartsWith("SELECT COUNT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "sample_entity", "doc_number"), result);
        }
    }
}
