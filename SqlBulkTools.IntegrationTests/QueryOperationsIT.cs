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
                .ForSimpleUpdateQuery(new Book() { Price = 100 })
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .Update()
                .Where(x => x.ISBN == isbn);


            int updatedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
        }

        [Test]
        public void SqlBulkTools_UpdateQuery_SetPriceAndDescriptionOnSingleEntity()
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
                .ForSimpleUpdateQuery(new Book()
                {
                    Price = 100,
                    Description = "Somebody will want me now! Yay"
                })
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .Update()
                .Where(x => x.ISBN == isbn);


            int updatedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(updatedRecords == 1);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
            Assert.AreEqual("Somebody will want me now! Yay", _db.Books.Single(x => x.ISBN == isbn).Description);
        }

        [Test]
        public void SqlBulkTools_UpdateQuery_MultipleConditionsTrue()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<Book>()
                .ForSimpleUpdateQuery(new Book() { Price = 100, WarehouseId = 5 })
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .AddColumn(x => x.WarehouseId)
                .Update()
                .Where(x => x.ISBN == isbn)
                .And(x => x.Price == 15);


            int updatedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(1, updatedRecords);
            Assert.AreEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
            Assert.AreEqual(5, _db.Books.Single(x => x.ISBN == isbn).WarehouseId);
        }

        [Test]
        public void SqlBulkTools_UpdateQuery_MultipleConditionsFalse()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 20)
                {
                    books[i].Price = 15;
                }
                else
                    books[i].Price = 25;
            }

            var bookToTest = books[5];
            var isbn = bookToTest.ISBN;

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Update price to 100

            bulk.Setup<Book>()
                .ForSimpleUpdateQuery(new Book() { Price = 100, WarehouseId = 5 })
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .AddColumn(x => x.WarehouseId)
                .Update()
                .Where(x => x.ISBN == isbn)
                .And(x => x.Price == 16);


            int updatedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(updatedRecords == 0);
            Assert.AreNotEqual(100, _db.Books.Single(x => x.ISBN == isbn).Price);
            Assert.AreNotEqual(5, _db.Books.Single(x => x.ISBN == isbn).WarehouseId);
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

        [Test]
        public void SqlBulkTools_DeleteQuery_DeleteWhenNotNullWithSchema()
        {
            _db.SchemaTest2.RemoveRange(_db.SchemaTest2.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<SchemaTest2>()
                .ForCollection(col)
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<SchemaTest2>()
                .ForSimpleDeleteQuery()
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .Delete()
                .Where(x => x.ColumnA != null);

            int deletedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(0, _db.SchemaTest2.Count());
        }



        [Test]
        public void SqlBulkTools_DeleteQuery_DeleteWhenNullWithWithSchema()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            List<SchemaTest2> col = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest2() { ColumnA = null });
            }

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<SchemaTest2>()
                .ForCollection(col)
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<SchemaTest2>()
                .ForSimpleDeleteQuery()
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .Delete()
                .Where(x => x.ColumnA == null);

            int deletedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(0, _db.SchemaTest2.Count());
        }

        [Test]
        public void SqlBulkTools_DeleteQuery_DeleteWithMultipleConditions()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            for (int i = 0; i < books.Count; i++)
            {
                if (i < 6)
                {
                    books[i].Price = 1 + (i * 100);
                    books[i].WarehouseId = 1;
                    books[i].Description = null;
                }
            }

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
                .Where(x => x.WarehouseId == 1)
                .And(x => x.Price >= 100)
                .And(x => x.Description == null);

            int deletedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(5, deletedRecords);
            Assert.AreEqual(25, _db.Books.Count());
        }

        [Test]
        public void SqlBulkTools_Insert_ManualAddColumn()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>()
                .ForSimpleInsertQuery(new Book() { BestSeller = true, Description = "Greatest dad in the world", Title = "Hello World", ISBN = "1234567", Price = 23.99M })
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.BestSeller)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Price)
                .Insert();

            int insertedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Insert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>()
                .ForSimpleInsertQuery(new Book()
                {
                    BestSeller = true,
                    Description = "Greatest dad in the world",
                    Title = "Hello World",
                    ISBN = "1234567",
                    Price = 23.99M
                })
                .WithTable("Books")
                .AddAllColumns()
                .Insert()
                .SetIdentityColumn(x => x.Id);

            int insertedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Upsert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>()
                .ForSimpleUpsertQuery(new Book()
                {
                    BestSeller = true,
                    Description = "Greatest dad in the world",
                    Title = "Hello World",
                    ISBN = "1234567",
                    Price = 23.99M
                })
                .WithTable("Books")
                .AddAllColumns()
                .Insert()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.Id);

            int insertedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Upsert_AddAllColumnsWithExistingRecord()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = default(int);

            bulk.Setup<Book>()
                .ForSimpleInsertQuery(new Book()
                {
                    BestSeller = true,
                    Description = "Greatest dad in the world",
                    Title = "Hello World",
                    ISBN = "1234567",
                    Price = 23.99M
                })
                .WithTable("Books")
                .AddAllColumns()
                .Insert()
                .SetIdentityColumn(x => x.Id);

            insertedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(insertedRecords == 1);

            //TODO use output identity column
            var insertedId = _db.Books.First().Id;

            bulk.Setup<Book>()
                .ForSimpleUpsertQuery(new Book()
                {
                    Id = insertedId,
                    BestSeller = true,
                    Description = "Greatest dad in the world",
                    Title = "Hello Greggo",
                    ISBN = "1234567",
                    Price = 23.99M
                })
                .WithTable("Books")
                .AddAllColumns()
                .Insert()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.Id);

            insertedRecords = bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.AreEqual(1, _db.Books.Count());
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.Title == "Hello Greggo"));
        }
    }
}