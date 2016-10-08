using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Migrations.Model;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.IntegrationTests.Model;
using TestContext = SqlBulkTools.IntegrationTests.Data.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class BulkOpearationsIT
    {
        private const int RepeatTimes = 1;

        private BookRandomizer _randomizer;
        private TestContext _db;
        private List<Book> _bookCollection;
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

        [TestCase(1000)]
        public void SqlBulkTools_BulkInsert(int rows)
        {
            BulkDelete(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsert(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(1000)]
        public void SqlBulkTools_BulkInsert_WithAllColumns(int rows)
        {
            BulkDelete(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsertAllColumns(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(500, 500)]
        public void SqlBulkTools_BulkInsertOrUpdate(int rows, int newRows)
        {
            BulkDelete(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                long time = BulkInsertOrUpdate(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(500, 500)]
        public void SqlBulkTools_BulkInsertOrUpdateAllColumns(int rows, int newRows)
        {
            BulkDelete(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsertOrUpdateAllColumns with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                long time = BulkInsertOrUpdateAllColumns(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(1000)]
        public void SqlBulkTools_BulkUpdate(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            BulkDelete(_db.Books.ToList());

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkUpdate with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                BulkInsert(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = BulkUpdate(_bookCollection);
                results.Add(time);

                var testUpdate = _db.Books.FirstOrDefault();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
                Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

                BulkDelete(_bookCollection);
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(1000)]
        public void SqlBulkTools_BulkUpdateOnIdentityColumn(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());
            BulkOperations bulk = new BulkOperations();

            BulkDelete(_db.Books.ToList());

         
            _bookCollection = _randomizer.GetRandomCollection(rows);

            bulk.Setup<Book>()
                .ForCollection(_bookCollection)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Update half the rows
            for (int j = 0; j < rows / 2; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                var prevId = _bookCollection[j].Id;
                _bookCollection[j] = newBook;
                _bookCollection[j].Id = prevId;

            }

            bulk.Setup<Book>()
                .ForCollection(_bookCollection)
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id)
                .SetIdentityColumn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var testUpdate = _db.Books.FirstOrDefault();
            Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
            Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
            Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);
        }

        [TestCase(1000)]
        public void SqlBulkTools_BulkDelete(int rows)
        {
            _bookCollection = _randomizer.GetRandomCollection(rows);
            BulkDelete(_db.Books.ToList());

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkDelete with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);
                long time = BulkDelete(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [Test]
        public void SqlBulkTools_TransactionRollsbackOnError()
        {
            BulkDelete(_db.Books.ToList());

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            _bookCollection = _randomizer.GetRandomCollection(20);
            BulkInsert(_bookCollection);

            var prevBook = _bookCollection[0];

            var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
            var prevIsbn = _bookCollection[0].ISBN;

            // Try to change the first element
            _bookCollection[0] = newBook;
            _bookCollection[0].ISBN = prevIsbn;

            // Force error at element 10. Price is a required field
            _bookCollection.ElementAt(10).Price = null;

            try
            {
                BulkUpdate(_bookCollection);
            }
            catch
            {
                // Validate that first element has not changed
                var firstElement = _db.Books.FirstOrDefault();
                Assert.AreEqual(firstElement.Price, prevBook.Price);
                Assert.AreEqual(firstElement.Title, prevBook.Title);
            }
        }

        [Test]
        public void SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);

            bulk.Setup<Book>()
                .ForCollection(_bookCollection)
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id);

            // Act & Assert
            Assert.Throws<IdentityException>(() => bulk.CommitTransaction("SqlBulkToolsTest"));

        }

        [Test]
        public void SqlBulkTools_IdentityColumnSet_UpdatesTargetWhenSetIdentityColumn()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);
            string testDesc = "New Description";

            BulkInsert(_bookCollection);

            _bookCollection = _db.Books.ToList();
            _bookCollection.First().Description = testDesc;

            bulk.Setup<Book>()
                .ForCollection(_bookCollection)
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.Id);

            // Act
            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.AreEqual(testDesc, _db.Books.First().Description);
        }

        [Test]
        public void SqlBulkTools_WithConflictingTableName_DeletesAndInsertsToCorrectTable()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest2> conflictingSchemaCol = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            // Act            
            bulk.Setup<SchemaTest2>()
                .ForCollection(_db.SchemaTest2)
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkDelete(); // Remove existing rows

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<SchemaTest2>()
                .ForCollection(conflictingSchemaCol)
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkInsert(); // Add new rows

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.IsTrue(_db.SchemaTest2.Any());

        }

        [Test]
        public void SqlBulkTools_BulkDeleteOnId_AddItemsThenRemovesAllItems()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest1> col = new List<SchemaTest1>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest1() { ColumnB = "ColumnA " + i });
            }

            // Act

            bulk.Setup<SchemaTest1>()
                .ForCollection(col)
                .WithTable("SchemaTest") // Don't specify schema. Default schema dbo is used. 
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            var allItems = _db.SchemaTest1.ToList();

            bulk.Setup<SchemaTest1>()
                .ForCollection(allItems)
                .WithTable("SchemaTest")
                .AddColumn(x => x.Id)
                .BulkDelete()
                .MatchTargetOn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert

            Assert.IsFalse(_db.SchemaTest1.Any());
        }

        [Test]
        public void SqlBulkTools_BulkUpdate_PartialUpdateOnlyUpdatesSelectedColumns()
        {
            // Arrange
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(30);

            BulkDelete(_db.Books.ToList());
            BulkInsert(_bookCollection);

            // Update just the price on element 5
            int elemToUpdate = 5;
            decimal updatedPrice = 9999999;
            var originalElement = _bookCollection.ElementAt(elemToUpdate);
            _bookCollection.ElementAt(elemToUpdate).Price = updatedPrice;

            // Act           
            bulk.Setup<Book>()
                .ForCollection(_bookCollection)
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.AreEqual(updatedPrice, _db.Books.Single(x => x.ISBN == originalElement.ISBN).Price);

            /* Profiler shows: MERGE INTO [SqlBulkTools].[dbo].[Books] WITH (HOLDLOCK) AS Target USING #TmpTable 
             * AS Source ON Target.ISBN = Source.ISBN WHEN MATCHED THEN UPDATE SET Target.Price = Source.Price, 
             * Target.ISBN = Source.ISBN ; DROP TABLE #TmpTable; */

        }

        [Test]
        public void SqlBulkTools_BulkInsertWithColumnMappings_CorrectlyMapsColumns()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            bulk.Setup<CustomColumnMappingTest>()
                .ForCollection(col)
                .WithTable("CustomColumnMappingTests")
                .AddAllColumns()
                .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.Any());
        }

        [Test]
        public void SqlBulkTools_WhenUsingReservedSqlKeywords()
        {
            _db.ReservedColumnNameTest.RemoveRange(_db.ReservedColumnNameTest.ToList());
            BulkOperations bulk = new BulkOperations();

            var list = new List<ReservedColumnNameTest>();

            for (int i = 0; i < 30; i++)
            {
                list.Add(new ReservedColumnNameTest() { Key = i });
            }

            bulk.Setup<ReservedColumnNameTest>()
                .ForCollection(list)
                .WithTable("ReservedColumnNameTests")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.Id)
                .SetIdentityColumn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(_db.ReservedColumnNameTest.Any());

        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdate_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdateWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }



        [Test]
        public void SqlBulkTools_BulkInsert_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());

            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            _db.Books.AddRange(_randomizer.GetRandomCollection(60)); // Add some random items before test. 
            _db.SaveChanges();

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(80); // Random between random items before test and total items after test. 
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }



        [Test]
        public void SqlBulkTools_BulkInsertWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

            List<Book> books = _randomizer.GetRandomCollection(30);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [Test]
        public void SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

            using (
                var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString)
                )
            using (var command = new SqlCommand(
                "DBCC CHECKIDENT ('[dbo].[Books]', RESEED, 10);", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();
                command.ExecuteNonQuery();
            }

            List<Book> books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = books.First();
            var expected = 11;

            Assert.AreEqual(expected, test.Id);
        }


       

        [Test]
        public void SqlBulkTools_BulkUpdateWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [Test]
        public void SqlBulkTools_BulkInsertAddInvalidDataType_ThrowsSqlBulkToolsExceptionException()
        {
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.InvalidType)
                .BulkInsert();

            Assert.Throws<SqlBulkToolsException>(() => bulk.CommitTransaction("SqlBulkToolsTest"));
        }

        [Test]
        public void SqlBulkTools_BulkInsertWithGenericType()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            bulk.Setup()
            .ForCollection(
                _bookCollection.Select(
                    x => new { x.Description, x.ISBN, x.Id, x.Price }))
            .WithTable("Books")
            .AddColumn(x => x.Id)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.Price)
            .BulkInsert()
            .SetIdentityColumn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(_db.Books.Any());
        }

        [Test]
        public void SqlBulkTools_BulkInsertWithoutSetter_ThrowsMeaningfulException()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            bulk.Setup()
            .ForCollection(
                _bookCollection.Select(
                    x => new { x.Description, x.ISBN, x.Id, x.Price }))
            .WithTable("Books")
            .AddColumn(x => x.Id)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.Price)
            .BulkInsert()
            .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            Assert.Throws<SqlBulkToolsException>(() => bulk.CommitTransaction("SqlBulkToolsTest"), 
                "No setter method available on property 'Id'. Could not write output back to property.");
        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdateWithPrivateIdentityField_ThrowsMeaningfulException()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            List<BookWithPrivateIdentity> booksWithPrivateIdentity = new List<BookWithPrivateIdentity>();

            books.ForEach(x => booksWithPrivateIdentity.Add(new BookWithPrivateIdentity()
            {
                ISBN = x.ISBN,
                Description = x.Description,
                Price = x.Price
                
            }));

            bulk.Setup<BookWithPrivateIdentity>()
            .ForCollection(booksWithPrivateIdentity)
            .WithTable("Books")
            .AddColumn(x => x.Id)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.Price)
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN)
            .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            Assert.Throws<SqlBulkToolsException>(() => bulk.CommitTransaction("SqlBulkToolsTest"), 
                "No setter method available on property 'Id'. Could not write output back to property.");
        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdae_TestDataTypes()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

            var todaysDate = DateTime.Today;
            Guid guid = Guid.NewGuid();

            BulkOperations bulk = new BulkOperations();
            List<TestDataType> dataTypeTest = new List<TestDataType>()
            {
                new TestDataType()
                {
                    BigIntTest = 342324324324324324,
                    TinyIntTest = 126,
                    DateTimeTest = todaysDate,
                    DateTime2Test = new DateTime(2008, 12, 12, 10, 20, 30),
                    DateTest = new DateTime(2007, 7, 5, 20, 30, 10),
                    TimeTest = new TimeSpan(23, 32, 23),
                    SmallDateTimeTest = new DateTime(2005, 7, 14),
                    BinaryTest = new byte[] {0, 3, 3, 2, 4, 3},
                    VarBinaryTest = new byte[] {3, 23, 33, 243},
                    DecimalTest = 178.43M,
                    MoneyTest = 24333.99M,
                    SmallMoneyTest = 103.32M,
                    RealTest = 32.53F,
                    NumericTest = 154343.3434342M,
                    FloatTest = 232.43F,
                    FloatTest2 = 43243.34,
                    TextTest = "This is some text.",
                    GuidTest = guid,
                    CharTest = new char[] {'S', 'o', 'm', 'e' },
                    XmlTest = "<title>The best SQL Bulk tool</title>",
                    NCharTest = "SomeText",
                    ImageTest = new byte[] {3,3,32,4}
                }
            };

            bulk.Setup<TestDataType>()
                .ForCollection(dataTypeTest)
                .WithTable("TestDataTypes")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.TimeTest);

            bulk.CommitTransaction("SqlBulkToolsTest");


            using (
                var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString)
                )
            using (var command = new SqlCommand("SELECT TOP 1 * FROM [dbo].[TestDataTypes]", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Assert.AreEqual(232.43F, reader["FloatTest"]);
                        Assert.AreEqual(43243.34, reader["FloatTest2"]);
                        Assert.AreEqual(178.43M, reader["DecimalTest"]);
                        Assert.AreEqual(24333.99M, reader["MoneyTest"]);
                        Assert.AreEqual(103.32M, reader["SmallMoneyTest"]);
                        Assert.AreEqual(32.53M, reader["RealTest"]);
                        Assert.AreEqual(154343.3434342M, reader["NumericTest"]);
                        Assert.AreEqual(todaysDate, reader["DateTimeTest"]);
                        Assert.AreEqual(new DateTime(2008, 12, 12, 10, 20, 30), reader["DateTime2Test"]);
                        Assert.AreEqual(new DateTime(2005, 7, 14), reader["SmallDateTimeTest"]);
                        Assert.AreEqual(new DateTime(2007, 7, 5), reader["DateTest"]);
                        Assert.AreEqual(new TimeSpan(23, 32, 23), reader["TimeTest"]);
                        Assert.AreEqual(guid, reader["GuidTest"]);
                        Assert.AreEqual("This is some text.", reader["TextTest"]);
                        Assert.AreEqual(new char[] { 'S', 'o', 'm', 'e' }, reader["CharTest"].ToString().Trim());
                        Assert.AreEqual(126, reader["TinyIntTest"]);
                        Assert.AreEqual(342324324324324324, reader["BigIntTest"]);
                        Assert.AreEqual("<title>The best SQL Bulk tool</title>", reader["XmlTest"]);
                        Assert.AreEqual("SomeText", reader["NCharTest"].ToString().Trim());
                        Assert.AreEqual(new byte[] { 3, 3, 32, 4 }, reader["ImageTest"]);
                        Assert.AreEqual(new byte[] { 0, 3, 3, 2, 4, 3 }, reader["BinaryTest"]);
                        Assert.AreEqual(new byte[] { 3, 23, 33, 243 }, reader["VarBinaryTest"]);
                    }
                }
            }


        }

        [Test, Ignore("You probably don't have this table or sproc. Anyway, this table contained around 35 million records. Bulk update took around 500ms for 853 records")]
        public async Task AdventureWorksTest()
        {

            List <Transaction> transactions = new List<Transaction>();
            using (var sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["AdventureWorks2014"].ConnectionString))
            using (
                var cmd =
                    new SqlCommand("GetTransactions", sqlConnection))
            {
                sqlConnection.Open();
                cmd.CommandType = CommandType.StoredProcedure;
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    Transaction trans = new Transaction()
                    {
                        ActualCost = (decimal)reader["ActualCost"],
                        ProductId = (int)reader["ProductID"],
                        TransactionId = (int)reader["TransactionID"],
                        TransactionDate = (DateTime)reader["TransactionDate"],
                        Quantity = (int)reader["Quantity"]
                    };

                    transactions.Add(trans);
                }

            }

            transactions.ForEach(x =>
            {
                x.ActualCost += (decimal) 0.43;
                x.Quantity += 32;
                x.TransactionDate = DateTime.UtcNow;
            } );

            IBulkOperations bulkOperations = new BulkOperations();
            IBulkOperations bulkOperations2 = new BulkOperations();

            bulkOperations.Setup<Transaction>()
                .ForCollection(transactions)                
                .WithTable("bigTransactionHistory")
                .AddColumn(x => x.TransactionId)
                .AddColumn(x => x.ActualCost)
                .AddColumn(x => x.ProductId)
                .AddColumn(x => x.Quantity)
                .AddColumn(x => x.TransactionDate)
                 .CustomColumnMapping(x => x.TransactionId, "TransactionID")
                .CustomColumnMapping(x => x.ProductId, "ProductID")
                .BulkUpdate()
                

                .MatchTargetOn(x => x.TransactionId);

            var watch = System.Diagnostics.Stopwatch.StartNew();


            var cTask = bulkOperations.CommitTransactionAsync("AdventureWorks2014");

            transactions.ForEach(x =>
            {
                x.ActualCost += (decimal)0.43;
                x.Quantity += 32 * 2;
                x.TransactionDate = DateTime.UtcNow;
            });

            bulkOperations2.Setup<Transaction>()
                .ForCollection(transactions)
                .WithTable("bigTransactionHistory")
                .AddAllColumns()
                .CustomColumnMapping(x => x.TransactionId, "TransactionID")
                .CustomColumnMapping(x => x.ProductId, "ProductID")
                .BulkUpdate()
                .MatchTargetOn(x => x.TransactionId);

            
            var lTask = bulkOperations2.CommitTransactionAsync("AdventureWorks2014");

            await cTask;
            await lTask;

            watch.Stop();

            // Add breakpoint here
            var elapsedMs = watch.ElapsedMilliseconds;

            Assert.IsTrue(transactions.Any());
            

        }



        private long BulkInsert(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddAllColumns()
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertOrUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkInsertOrUpdateAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.PublishDate)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkDelete(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }



    }
}