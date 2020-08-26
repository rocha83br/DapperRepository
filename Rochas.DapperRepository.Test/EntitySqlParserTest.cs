using Rochas.DapperRepository.Enums;
using Rochas.DapperRepository.Helpers;
using System;
using System.Collections.Generic;
using Xunit;

namespace Rochas.DapperRepository.Test
{
    public class EntitySqlParserTest
    {
        [Fact]
        public void GetTest()
        {;
            var testFilter = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(testFilter, PersistenceAction.Get, testFilter);

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "dbo.SampleEntity", "DocNumber"), result.Trim());
        }

        [Fact]
        public void ListTest()
        {
            var testFilter = new SampleEntity() { Name = "roberto" };
            var result = EntitySqlParser.ParseEntity(testFilter, PersistenceAction.List, testFilter);

            Assert.NotNull(result);
            Assert.StartsWith("SELECT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} LIKE '%roberto%'", "dbo.SampleEntity", "Name"), result.Trim());
        }

        [Fact]
        public void CreateTest()
        {
            var sampleEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(sampleEntity, PersistenceAction.Create);

            Assert.NotNull(result);
            Assert.StartsWith("INSERT INTO", result);
            Assert.Contains("VALUES", result);
            Assert.Contains("CreationDate", result);
            Assert.Contains("Name", result);
            Assert.Contains("Active", result);
        }

        [Fact]
        public void EditTest()
        {
            var editedEntity = new SampleEntity() { Name = "roberto gomes", Age = 35 };
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(editedEntity, PersistenceAction.Edit, filterEntity);

            Assert.NotNull(result);
            Assert.StartsWith("UPDATE", result);
            Assert.Contains("SET", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "dbo.SampleEntity", "DocNumber"), result.Trim());
        }

        [Fact]
        public void DeleteTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, PersistenceAction.Delete, filterEntity);

            Assert.NotNull(result);
            Assert.StartsWith("DELETE FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "dbo.SampleEntity", "DocNumber"), result.Trim());
        }

        [Fact]
        public void CountTest()
        {
            var filterEntity = new SampleEntity() { DocNumber = 12345 };
            var result = EntitySqlParser.ParseEntity(filterEntity, PersistenceAction.Count, filterEntity);

            Assert.NotNull(result);
            Assert.StartsWith("SELECT COUNT", result);
            Assert.Contains("FROM", result);
            Assert.EndsWith(string.Format("WHERE {0}.{1} = 12345", "dbo.SampleEntity", "DocNumber"), result.Trim());
        }
    }
}
