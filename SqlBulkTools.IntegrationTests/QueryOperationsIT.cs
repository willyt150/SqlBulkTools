using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Transactions;
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
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Update price to 100

                    updatedRecords = bulk.Setup<Book>()
                        .ForSimpleUpdateQuery(new Book() { Price = 100 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .Commit(conn);

                }

                trans.Complete();
            }

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

            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    // Update price to 100

                    updatedRecords = bulk.Setup<Book>()
                        .ForSimpleUpdateQuery(new Book()
                        {
                            Price = 100,
                            Description = "Somebody will want me now! Yay"
                        })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.Description)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .Commit(conn);

                }

                trans.Complete();
            }



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
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    updatedRecords = bulk.Setup<Book>()
                        .ForSimpleUpdateQuery(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 15)
                        .Commit(conn);
                }

                trans.Complete();
            }

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
            int updatedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert();

                    // Update price to 100

                    updatedRecords = bulk.Setup<Book>()
                        .ForSimpleUpdateQuery(new Book() { Price = 100, WarehouseId = 5 })
                        .WithTable("Books")
                        .AddColumn(x => x.Price)
                        .AddColumn(x => x.WarehouseId)
                        .Update()
                        .Where(x => x.ISBN == isbn)
                        .And(x => x.Price == 16)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.IsTrue(updatedRecords == 0);
        }

        [Test]
        public void SqlBulkTools_DeleteQuery_DeleteSingleEntity()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            var bookIsbn = books[5].ISBN;
            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    deletedRecords = bulk.Setup<Book>()
                        .ForSimpleDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.ISBN == bookIsbn)
                        .Commit(conn);

                }

                trans.Complete();
            }


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

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);


                    bulk.Setup<SchemaTest2>()
                        .ForSimpleDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA != null)
                        .Commit(conn);

                }

                trans.Complete();
            }


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

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<SchemaTest2>()
                        .ForCollection(col)
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    bulk.Setup<SchemaTest2>()
                        .ForSimpleDeleteQuery()
                        .WithTable("SchemaTest")
                        .WithSchema("AnotherSchema")
                        .Delete()
                        .Where(x => x.ColumnA == null)
                        .Commit(conn);
                }

                trans.Complete();
            }

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

            int deletedRecords = 0;

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    bulk.Setup<Book>()
                        .ForCollection(books)
                        .WithTable("Books")
                        .AddAllColumns()
                        .BulkInsert()
                        .Commit(conn);

                    deletedRecords = bulk.Setup<Book>()
                        .ForSimpleDeleteQuery()
                        .WithTable("Books")
                        .Delete()
                        .Where(x => x.WarehouseId == 1)
                        .And(x => x.Price >= 100)
                        .And(x => x.Description == null)
                        .Commit(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(5, deletedRecords);
            Assert.AreEqual(25, _db.Books.Count());
        }

        [Test]
        public void SqlBulkTools_Insert_ManualAddColumn()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = bulk.Setup<Book>()
                        .ForSimpleInsertQuery(new Book() { BestSeller = true, Description = "Greatest dad in the world", Title = "Hello World", ISBN = "1234567", Price = 23.99M })
                        .WithTable("Books")
                        .AddColumn(x => x.Title)
                        .AddColumn(x => x.ISBN)
                        .AddColumn(x => x.BestSeller)
                        .AddColumn(x => x.Description)
                        .AddColumn(x => x.Price)
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput)
                        .Commit(conn);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Insert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();
            int insertedRecords = 0;
            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    insertedRecords = bulk.Setup<Book>()
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
                        .SetIdentityColumn(x => x.Id)
                        .Commit(conn);

                }

                trans.Complete();
            }

            Assert.AreEqual(1, insertedRecords);
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Upsert_AddAllColumns()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            using (TransactionScope tx = new TransactionScope())
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {                                     
                    var bulk = new BulkOperations();
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
                    .Upsert()
                    .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput)
                    .MatchTargetOn(x => x.Id)
                    .Commit(con);
                }

                tx.Complete();
            }

            Assert.AreEqual(1, _db.Books.Count());
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.ISBN == "1234567"));
        }

        [Test]
        public void SqlBulkTools_Upsert_AddAllColumnsWithExistingRecord()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection con = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {

                    Book book = new Book()
                    {
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello World",
                        ISBN = "1234567",
                        Price = 23.99M
                    };

                    bulk.Setup<Book>()
                        .ForSimpleInsertQuery(book)
                        .WithTable("Books")
                        .AddAllColumns()
                        .Insert()
                        .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput)
                        .Commit(con);

                    bulk.Setup<Book>()
                    .ForSimpleUpsertQuery(new Book()
                    {
                        Id = book.Id,
                        BestSeller = true,
                        Description = "Greatest dad in the world",
                        Title = "Hello Greggo",
                        ISBN = "1234567",
                        Price = 23.99M
                    })
                    .WithTable("Books")
                    .AddAllColumns()
                    .Upsert()
                    .SetIdentityColumn(x => x.Id)
                    .MatchTargetOn(x => x.Id)
                    .Commit(con);
                }

                trans.Complete();
            }

            Assert.AreEqual(1, _db.Books.Count());
            Assert.IsNotNull(_db.Books.SingleOrDefault(x => x.Title == "Hello Greggo"));
        }

        [Test]
        public void SqlBulkTools_Insert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = $"ColumnX 1",
                ColumnYIsDifferentInDatabase = 1
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForSimpleInsertQuery(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Insert()
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnXIsDifferent == "ColumnX 1");
        }

        [Test]
        public void SqlBulkTools_Upsert_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 3
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForSimpleUpsertQuery(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()                        
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")                        
                        .Upsert()
                        .MatchTargetOn(x => x.NaturalId)
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnYIsDifferentInDatabase == 3);
        }

        [Test]
        public void SqlBulkTools_Update_CustomColumnMapping()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            var customColumn = new CustomColumnMappingTest()
            {
                NaturalId = 1,
                ColumnXIsDifferent = "ColumnX " + 1,
                ColumnYIsDifferentInDatabase = 1
            };

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            using (TransactionScope trans = new TransactionScope())
            {
                using (SqlConnection conn = new SqlConnection(ConfigurationManager
                    .ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
                {
                    bulk.Setup<CustomColumnMappingTest>()
                        .ForSimpleInsertQuery(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Insert()
                        .Commit(conn);

                    customColumn.ColumnXIsDifferent = "updated";
                    

                    bulk.Setup<CustomColumnMappingTest>()
                        .ForSimpleUpdateQuery(customColumn)
                        .WithTable("CustomColumnMappingTests")
                        .AddAllColumns()
                        .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                        .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                        .Update()
                        .Where(x => x.NaturalId == 1)
                        .Commit(conn);
                }

                trans.Complete();
            }

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.First().ColumnXIsDifferent == "updated");
        }
    }
}