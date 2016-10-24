using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.IntegrationTests.Model;
using TestContext = SqlBulkTools.IntegrationTests.Data.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class QueryOperationsIT
    {
        private const int RepeatTimes = 1;

        private BookRandomizer _randomizer;
        private TestContext _db;

        [OneTimeSetUp]
        public void Setup()
        {
            _db = new TestContext();
            _randomizer = new BookRandomizer();
            Database.SetInitializer(new DatabaseInitialiser());
            _db.Database.Initialize(true);
            FileHelper.DeleteLogFile();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _db.Dispose();
        }

        [Test]
        public void SqlBulkTools_UpdateQuery_SetPriceOnSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookToTest = books[5];
            bookToTest.Price = 50;
            var isbn = bookToTest.ISBN;

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Update price to 100

            bulk.Setup<Book>()
                .ForSimpleUpdateQuery(new Book(){Price = 100})
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .Update()
                .Where(x => x.ISBN == isbn);
            

            int updatedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
        }

        [Test]
        public void SqlBulkTools_DeleteQuery_DeleteSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookIsbn = books[5].ISBN;

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<Book>()
                .ForSimpleDeleteQuery()
                .WithTable("Books")
                .Delete()
                .Where(x => x.ISBN == bookIsbn);

            int deletedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(deletedRecords == 1);
            Assert.AreEqual(29, _db.Books.Count());
        }

    }
}