using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;
using Rochas.DapperRepository.Enums;

namespace Rochas.DapperRepository.Test
{
    public class GenericRepositoryTest
    {
        private string databaseFileName = "MockDatabase.sqlite";
        private string connString = $"Data Source=MockDatabase.sqlite;Version=3;New=True;";

        [Fact]
        public void Test01_Initialize()
        {            
            var tableScript = @"CREATE TABLE [sample_entity](
	                                         [doc_number] [int] PRIMARY KEY NOT NULL,
	                                         [creation_date] [datetime] NOT NULL,
	                                         [name] [varchar](200) NOT NULL,
	                                         [age] [int] NULL,
	                                         [height] [decimal](18, 2) NULL,
	                                         [weight] [decimal](18, 2) NULL,
	                                         [active] [bit] NOT NULL)";
            
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                repos.Initialize(databaseFileName, tableScript);
            }

            Assert.True(File.Exists(databaseFileName));
        }

        [Fact]
        public void Test02_Create()
        {
            int result;
            var sampleEntity = new SampleEntity() {
                DocNumber = 12345,
                CreationDate = DateTime.Now,
                Name = "Roberto Torres",
                Active = true
            };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Create(sampleEntity);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test03_GetByKey()
        {
            SampleEntity result;

            var key = 12345;
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Get(key);
            }

            Assert.NotNull(result);
            Assert.Equal(key, result.DocNumber);
        }

        [Fact]
        public void Test04_GetByFilter()
        {
            SampleEntity result;

            var filter = new SampleEntity() { DocNumber = 12345 };
            using(var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Get(filter);
            }

            Assert.NotNull(result);
            Assert.Equal(filter.DocNumber, result.DocNumber);
        }

        [Fact]
        public void Test05_List()
        {
            ICollection<SampleEntity> result;

            var filter = new SampleEntity() { Name = "roberto" };
            
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.List(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test06_Search()
        {
            ICollection<SampleEntity> result;

            var filter = "torres";

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Search(filter);
            }

            Assert.NotNull(result);
            Assert.True(result.Any());
        }

        [Fact]
        public void Test07_Count()
        {
            int result = 0;
            var filter = new SampleEntity() { Name = "roberto" };

            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Count(filter);
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test08_Edit()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                var item = repos.Get(filter);
                if (item != null)
                {
                    item.Age = 37;
                    result = repos.Edit(item, filter);
                }
            }

            Assert.True(result > 0);
        }

        [Fact]
        public void Test09_Delete()
        {
            int result = 0;
            var filter = new SampleEntity() { DocNumber = 12345 };
            using (var repos = new GenericRepository<SampleEntity>(DatabaseEngine.SQLite, connString))
            {
                result = repos.Delete(filter);
            }

            Assert.True(result > 0);
        }
    }
}
